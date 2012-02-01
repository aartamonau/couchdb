% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_set_index_iter).

new() ->
    ok.

worker(DDoc, ViewName, ViewSpec, IndexMergeParams, UserCtx) ->
    #index_merge{
       extra = Extra,
       http_params = ViewArgs
    } = IndexMergeParams,

    ViewType = view_type(DDoc, ViewName, ViewArgs),

    FoldFun = case ViewType of
    reduce ->
        fun reduce_set_view_folder/5;
    _ when ViewType =:= map; ViewType =:= red_map ->
        fun map_set_view_folder/5
    end,

    try
        case get_view_and_group(ViewSpec, ViewArgs, ViewType) of
        {ok, View, Group} ->
            case good_ddoc(DDocDb, DDoc, MergeParams) of
            true ->
               ok;
            false ->
                couch_index_iter:init_ack({error, revision_mismatch})
            end
        Error ->
            couch_index_iter:init_ack(Error)
        end
    after
        couch_index_iter:done()
    end.

    try
        case good_ddoc(DDocDb, DDoc, MergeParams) of
        true ->
            couch_index_iter:init_ack(ok),
            actual_worker(FoldFun, IndexSpec, MergeParams, DDoc, UserCtx);
        false ->
            couch_index_iter:init_ack({error, revision_mismatch})
        end
    catch
    ddoc_db_not_found ->
        couch_index_iter:yield(
          {error, ?LOCAL, ddoc_not_found_msg(DDocDbName, DDocId)});
    _Tag:Error ->
        Stack = erlang:get_stacktrace(),
        ?LOG_ERROR("Caught unexpected error while serving "
                   "index query for `~s/~s`:~n~p", [DbName, DDocId, Stack]),

        couch_index_iter:yield(parse_error(Error))
    after
        couch_index_iter:done()
    end.

get_view_and_group(ViewSpec, ViewArgs, ViewType) ->
    case (ViewSpec#set_view_spec.view =/= nil) andalso
        (ViewSpec#set_view_spec.group =/= nil) of
    true ->
        {ViewSpec#set_view_spec.view, ViewSpec#set_view_spec.group};
    false ->
        case ViewType =:= reduce of
        true ->
            prepare_set_view(ViewSpec, ViewArgs, Queue,
                             fun couch_set_view:get_reduce_view/5);
        false ->
            case prepare_set_view(
                ViewSpec, ViewArgs, fun couch_set_view:get_map_view/5) of
            {error, not_found} ->
                case prepare_set_view(
                    ViewSpec, ViewArgs, fun couch_set_view:get_reduce_view/5) of
                {RedView, Group0} ->
                    {couch_set_view:extract_map_view(RedView), Group0};
                Else ->
                    Else
                end;
            Else ->
                Else
            end
        end
    end.

map_set_view_folder(ViewSpec, MergeParams, UserCtx, DDoc) ->
    #set_view_spec{
        name = SetName, ddoc_id = DDocId
    } = ViewSpec,
    #index_merge{
        http_params = ViewArgs
    } = MergeParams,
    #view_query_args{
        include_docs = IncludeDocs,
        conflicts = Conflicts
    } = ViewArgs,

    DDocDbName = ?master_dbname(SetName),

    PrepareResult = case (ViewSpec#set_view_spec.view =/= nil) andalso
        (ViewSpec#set_view_spec.group =/= nil) of
    true ->
        {ViewSpec#set_view_spec.view, ViewSpec#set_view_spec.group};
    false ->
        case prepare_set_view(
            ViewSpec, ViewArgs, fun couch_set_view:get_map_view/5) of
        not_found ->
            case prepare_set_view(
                ViewSpec, ViewArgs, fun couch_set_view:get_reduce_view/5) of
            {RedView, Group0} ->
                {couch_set_view:extract_map_view(RedView), Group0};
            Else ->
                Else
            end;
        Else ->
            Else
        end
    end,

    case PrepareResult of
    error ->
        %%  handled by prepare_set_view
        ok;
    {View, Group} ->
        yield_debug_info(ViewArgs, Group),
        try
            FoldFun = make_map_set_fold_fun(IncludeDocs, Conflicts, SetName,
                UserCtx),

            case not(couch_index_merger:should_check_rev(MergeParams, DDoc)) orelse
                couch_index_merger:ddoc_unchanged(DDocDbName, DDoc) of
            true ->
                {ok, RowCount} = couch_set_view:get_row_count(View),
                couch_index_iter:init_ack(ok),
                couch_index_iter:yield({row_count, RowCount}),
                {ok, _, _} = couch_set_view:fold(Group, View, FoldFun, [], ViewArgs);
            false ->
                couch_index_iter:init_ack({error, revision_mismatch})
                %% couch_index_iter:yield(revision_mismatch)
            end
        catch
        ddoc_db_not_found ->
            ok = couch_view_merger_queue:queue(
                Queue, {error, ?LOCAL,
                    couch_index_merger:ddoc_not_found_msg(DDocDbName, DDocId)});
        _Tag:Error ->
            Stack = erlang:get_stacktrace(),
            ?LOG_ERROR("Caught unexpected error "
                       "while serving view query ~s/~s: ~p~n~p",
                       [SetName, DDocId, Error, Stack]),
            couch_view_merger_queue:queue(Queue, {error, ?LOCAL, to_binary(Error)})
        after
            couch_set_view:release_group(Group),
            ok = couch_view_merger_queue:done(Queue)
        end
    end.

reduce_set_view_folder(ViewSpec, MergeParams, DDoc) ->
    #set_view_spec{
        name = SetName, ddoc_id = DDocId
    } = ViewSpec,
    #index_merge{
        http_params = ViewArgs
    } = MergeParams,

    DDocDbName = ?master_dbname(SetName),
    PrepareResult = case (ViewSpec#set_view_spec.view =/= nil) andalso
        (ViewSpec#set_view_spec.group =/= nil) of
    true ->
        {ViewSpec#set_view_spec.view, ViewSpec#set_view_spec.group};
    false ->
        prepare_set_view(ViewSpec, ViewArgs, Queue, fun couch_set_view:get_reduce_view/5)
    end,

    case PrepareResult of
    error ->
        %%  handled by prepare_set_view
        ok;
    {View, Group} ->
        queue_debug_info(ViewArgs, Group, Queue),
        try
            FoldFun = make_reduce_fold_fun(ViewArgs, Queue),
            KeyGroupFun = make_group_rows_fun(ViewArgs),

            case not(couch_index_merger:should_check_rev(MergeParams, DDoc)) orelse
                couch_index_merger:ddoc_unchanged(DDocDbName, DDoc) of
            true ->
                {ok, _} = couch_set_view:fold_reduce(Group, View, FoldFun, [],
                                                     KeyGroupFun, ViewArgs);
            false ->
                ok = couch_view_merger_queue:queue(Queue, revision_mismatch)
            end
        catch
        ddoc_db_not_found ->
            ok = couch_view_merger_queue:queue(
                Queue, {error, ?LOCAL,
                    couch_index_merger:ddoc_not_found_msg(DDocDbName, DDocId)});
        _Tag:Error ->
            Stack = erlang:get_stacktrace(),
            ?LOG_ERROR("Caught unexpected error "
                       "while serving view query ~s/~s: ~p~n~p",
                       [SetName, DDocId, Error, Stack]),
            couch_view_merger_queue:queue(Queue, {error, ?LOCAL, to_binary(Error)})
        after
            couch_set_view:release_group(Group),
            ok = couch_view_merger_queue:done(Queue)
        end
    end.

get_set_view(GetSetViewFn, SetName, DDocId, ViewName, Stale, Partitions) ->
    case GetSetViewFn(SetName, DDocId, ViewName, ok, Partitions) of
    {ok, StaleView, StaleGroup, []} ->
        case Stale of
        ok ->
            {ok, StaleView, StaleGroup, []};
        _Other ->
            couch_set_view:release_group(StaleGroup),
            GetSetViewFn(SetName, DDocId, ViewName, Stale, Partitions)
        end;
    Other ->
        Other
    end.

prepare_set_view(ViewSpec, ViewArgs, GetSetViewFn) ->
    #set_view_spec{
        name = SetName,
        ddoc_id = DDocId, view_name = ViewName,
        partitions = Partitions
    } = ViewSpec,
    #view_query_args{
        stale = Stale
    } = ViewArgs,

    try
        case get_set_view(GetSetViewFn, SetName, DDocId,
                          ViewName, Stale, Partitions) of
        {ok, View, Group, []} ->
            {View, Group};
        {ok, _, Group, MissingPartitions} ->
            ?LOG_INFO("Set view `~s`, group `~s`, missing partitions: ~w",
                      [SetName, DDocId, MissingPartitions]),
            {error, index_outdated}
        {not_found, missing_named_view} ->
            {error, not_found}
        end
    catch
    view_undefined ->
        {error, view_undefined_msg(SetName, DDocId)}
    end.

make_map_set_fold_fun(false, _Conflicts, _SetName, _UserCtx, Queue) ->
    fun({{Key, DocId}, {PartId, Value}}, _, Acc) ->
        Kv = {{Key, DocId}, {PartId, Value}},
        ok = couch_view_merger_queue:queue(Queue, Kv),
        {ok, Acc}
    end;

make_map_set_fold_fun(true, Conflicts, SetName, UserCtx, Queue) ->
    DocOpenOpts = if Conflicts -> [conflicts]; true -> [] end,
    fun({{Key, DocId}, {PartId, Value}}, _, Acc) ->
        Kv = {{Key, DocId}, Value},
        JsonDoc = couch_set_view_http:get_row_doc(
                Kv, SetName, PartId, true, UserCtx, DocOpenOpts),
        Row = {{Key, DocId}, {PartId, Value}, {doc, JsonDoc}},
        ok = couch_view_merger_queue:queue(Queue, Row),
        {ok, Acc}
    end.

yield_debug_info(#view_query_args{debug = false}, _Group) ->
    ok;
yield_debug_info(_QueryArgs, #set_view_group{} = Group) ->
    #set_view_debug_info{
        original_abitmask = OrigMainAbitmask,
        original_pbitmask = OrigMainPbitmask,
        stats = Stats
    } = Group#set_view_group.debug_info,
    #set_view_group_stats{
        full_updates = FullUpdates,
        partial_updates = PartialUpdates,
        stopped_updates = StoppedUpdates,
        compactions = Compactions,
        cleanup_stops = CleanupStops,
        cleanups = Cleanups,
        updater_cleanups = UpdaterCleanups,
        update_history = UpdateHist,
        compaction_history = CompactHist,
        cleanup_history = CleanupHist
    } = Stats,
    OrigMainActive = couch_set_view_util:decode_bitmask(OrigMainAbitmask),
    ModMainActive = couch_set_view_util:decode_bitmask(?set_abitmask(Group)),
    OrigMainPassive = couch_set_view_util:decode_bitmask(OrigMainPbitmask),
    ModMainPassive = couch_set_view_util:decode_bitmask(?set_pbitmask(Group)),
    MainCleanup = couch_set_view_util:decode_bitmask(?set_cbitmask(Group)),
    % 0 padded so that a pretty print JSON can sanely sort the keys (partition IDs)
    IndexedSeqs = [{?l2b(io_lib:format("~4..0b", [P])), S} || {P, S} <- ?set_seqs(Group)],
    MainInfo = [
        {<<"active_partitions">>, ordsets:from_list(ModMainActive)},
        {<<"original_active_partitions">>, ordsets:from_list(OrigMainActive)},
        {<<"passive_partitions">>, ordsets:from_list(ModMainPassive)},
        {<<"original_passive_partitions">>, ordsets:from_list(OrigMainPassive)},
        {<<"cleanup_partitions">>, ordsets:from_list(MainCleanup)},
        {<<"indexed_seqs">>, {IndexedSeqs}},
        {<<"stats">>, {[
            {<<"full_updates">>, FullUpdates},
            {<<"partial_updates">>, PartialUpdates},
            {<<"stopped_updates">>, StoppedUpdates},
            {<<"compactions">>, Compactions},
            {<<"cleanup_stops">>, CleanupStops},
            {<<"cleanups">>, Cleanups},
            {<<"updater_cleanups">>, UpdaterCleanups},
            {<<"update_history">>, UpdateHist},
            {<<"cleanup_history">>, CleanupHist},
            {<<"compaction_history">>, CompactHist}
        ]}}
    ],
    RepInfo = replica_group_debug_info(Group),
    Info = {MainInfo ++ RepInfo},
    yield({debug_info, ?LOCAL, Info}).

replica_group_debug_info(#set_view_group{replica_group = nil}) ->
    [];
replica_group_debug_info(#set_view_group{replica_group = RepGroup}) ->
    #set_view_group{
        debug_info = #set_view_debug_info{
            original_abitmask = OrigRepAbitmask,
            original_pbitmask = OrigRepPbitmask,
            stats = Stats
        }
    } = RepGroup,
    #set_view_group_stats{
        full_updates = FullUpdates,
        partial_updates = PartialUpdates,
        stopped_updates = StoppedUpdates,
        compactions = Compactions,
        cleanup_stops = CleanupStops,
        cleanups = Cleanups,
        updater_cleanups = UpdaterCleanups,
        update_history = UpdateHist,
        compaction_history = CompactHist,
        cleanup_history = CleanupHist
    } = Stats,
    OrigRepActive = couch_set_view_util:decode_bitmask(OrigRepAbitmask),
    ModRepActive = couch_set_view_util:decode_bitmask(?set_abitmask(RepGroup)),
    OrigRepPassive = couch_set_view_util:decode_bitmask(OrigRepPbitmask),
    ModRepPassive = couch_set_view_util:decode_bitmask(?set_pbitmask(RepGroup)),
    RepCleanup = couch_set_view_util:decode_bitmask(?set_cbitmask(RepGroup)),
    % 0 padded so that a pretty print JSON can sanely sort the keys (partition IDs)
    IndexedSeqs = [{?l2b(io_lib:format("~4..0b", [P])), S} || {P, S} <- ?set_seqs(RepGroup)],
    [
        {<<"replica_active_partitions">>, ordsets:from_list(ModRepActive)},
        {<<"replica_original_active_partitions">>, ordsets:from_list(OrigRepActive)},
        {<<"replica_passive_partitions">>, ordsets:from_list(ModRepPassive)},
        {<<"replica_original_passive_partitions">>, ordsets:from_list(OrigRepPassive)},
        {<<"replica_cleanup_partitions">>, ordsets:from_list(RepCleanup)},
        {<<"replica_indexed_seqs">>, {IndexedSeqs}},
        {<<"replica_stats">>, {[
            {<<"full_updates">>, FullUpdates},
            {<<"partial_updates">>, PartialUpdates},
            {<<"stopped_updates">>, StoppedUpdates},
            {<<"compactions">>, Compactions},
            {<<"cleanup_stops">>, CleanupStops},
            {<<"cleanups">>, Cleanups},
            {<<"updater_cleanups">>, UpdaterCleanups},
            {<<"update_history">>, UpdateHist},
            {<<"cleanup_history">>, CleanupHist},
            {<<"compaction_history">>, CompactHist}
        ]}}
    ].
