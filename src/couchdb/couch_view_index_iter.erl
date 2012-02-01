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

-module(couch_view_index_iter).

new(DDoc, ViewName, IndexSpec, IndexMergeParams, UserCtx) ->
    couch_index_iter:new(
      ?MODULE, worker,
      [DDoc, ViewName, IndexSpec, IndexMergeParams, UserCtx]).

worker(DDoc, ViewName, IndexSpec, IndexMergeParams, UserCtx) ->
    #index_merge{
       extra = Extra,
       http_params = ViewArgs
    } = IndexMergeParams,

    ViewType = view_type(DDoc, ViewName, ViewArgs),

    try
        View = get_view(ViewType, Db, DDocDbName, DDocId, ViewName, Stale),

        case good_ddoc(DDocDbName, DDoc, MergeParams) of
        true ->
            couch_index_iter:init_ack(ok),

            try
                actual_worker(View, ViewType,
                              IndexSpec, MergeParams, DDoc, UserCtx)
            catch
            _Tag:Error ->
                Stack = erlang:get_stacktrace(),
                ?LOG_ERROR("Caught unexpected error while serving index query "
                           " for `~s/~s`:~n~p", [DbName, DDocId, Stack]),
                couch_index_iter:yield(parse_error(Error))
            end;
        false ->
            couch_index_iter:init_ack({error, revision_mismatch})
        end;
    catch
    %% can be thrown by get_view and good_ddoc
    ddoc_db_not_found ->
        couch_index_iter:init_ack({error, ddoc_db_not_found});
    after
        couch_index_iter:done()
    end.

actual_worker(ViewType, View, IndexSpec, MergeParams, DDoc, UserCtx) ->
    #simple_index_spec{
        database = DbName, ddoc_database = DDocDbName, ddoc_id = DDocId
    },

    case couch_db:open(DbName, [{user_ctx, UserCtx}]) of
    {ok, Db} ->
        try
            folder(ViewType, View, Db, IndexSpec, MergeParams, UserCtx, DDoc)
        catch
        {not_found, Reason} when Reason =:= missing; Reason =:= deleted ->
            couch_index_iter:yield(
              {error, ?LOCAL, ddoc_not_found_msg(DbName, DDocId)});
        after
            couch_db:close(Db)
        end;
    {not_found, _} ->
        couch_index_iter:yield({error, ?LOCAL, db_not_found_msg(DbName)})
    end.

good_ddoc(DDocDb, DDoc, MergeParams) ->
    not(couch_index_merger:should_check_rev(MergeParams, DDoc)) orelse
        couch_index_merger:ddoc_unchanged(DDocDb, DDoc).

map_view_folder(View, Db, #simple_index_spec{index_name = <<"_all_docs">>},
        MergeParams, _UserCtx, _DDoc) ->
    #index_merge{
        http_params = ViewArgs,
        extra = #view_merge{
            keys = Keys
        }
    } = MergeParams,
    {ok, Info} = couch_db:get_db_info(Db),
    couch_index_iter:yield({row_count, get_value(doc_count, Info)}),
    % TODO: add support for ?update_seq=true and offset
    fold_local_all_docs(Keys, Db, ViewArgs);

map_view_folder(View, Db, ViewSpec, MergeParams, _UserCtx, DDoc) ->
    #simple_index_spec{
        ddoc_database = DDocDbName, ddoc_id = DDocId, index_name = ViewName
    } = ViewSpec,
    #index_merge{
        http_params = ViewArgs,
        extra = #view_merge{
            keys = Keys
        }
    } = MergeParams,
    #view_query_args{
        stale = Stale,
        include_docs = IncludeDocs,
        conflicts = Conflicts
    } = ViewArgs,
    FoldlFun = make_map_fold_fun(IncludeDocs, Conflicts, Db),
    {DDocDb, View} = get_map_view(Db, DDocDbName, DDocId, ViewName, Stale),

    {ok, RowCount} = couch_view:get_row_count(View),
    couch_index_iter:yield({row_count, RowCount}),
    case Keys of
    nil ->
        FoldOpts = couch_httpd_view:make_key_options(ViewArgs),
        {ok, _, _} = couch_view:fold(View, FoldlFun, [], FoldOpts);
    _ when is_list(Keys) ->
        lists:foreach(
            fun(K) ->
                FoldOpts = couch_httpd_view:make_key_options(
                    ViewArgs#view_query_args{start_key = K, end_key = K}),
                {ok, _, _} = couch_view:fold(View, FoldlFun, [], FoldOpts)
            end,
            Keys)
    end,

    catch couch_db:close(DDocDb).

get_map_view(Db, DDocDbName, DDocId, ViewName, Stale) ->
    GroupId = couch_index_merger:get_group_id(DDocDbName, DDocId),
    View = case couch_view:get_map_view(Db, GroupId, ViewName, Stale) of
    {ok, MapView, _} ->
        MapView;
    {not_found, _} ->
        {ok, RedView, _} = couch_view:get_reduce_view(
            Db, GroupId, ViewName, Stale),
        couch_view:extract_map_view(RedView)
    end,
    case GroupId of
        {DDocDb, DDocId} ->
            catch couch_db:close(DDocDb),
            View;
        DDocId ->
            View
    end.

fold_local_all_docs(Keys, Db, ViewArgs) ->
    #view_query_args{
        direction = Dir,
        include_docs = IncludeDocs,
        conflicts = Conflicts
    } = ViewArgs,
    FoldFun = case Dir of
    fwd ->
        fun lists:foldl/3;
    rev ->
        fun lists:foldr/3
    end,
    FoldFun(
        fun(Key, _Acc) ->
            Row = case (catch couch_db:get_doc_info(Db, Key)) of
            {ok, #doc_info{} = DocInfo} ->
                all_docs_row(DocInfo, Db, IncludeDocs, Conflicts);
            not_found ->
                {{Key, error}, not_found}
            end,
            couch_index_iter:yield(Row)
        end, [], Keys).


all_docs_row(DocInfo, Db, IncludeDoc, Conflicts) ->
    #doc_info{id = Id, rev = Rev, deleted = Del} = DocInfo,
    Value = {[{<<"rev">>, couch_doc:rev_to_str(Rev)}] ++ case Del of
    true ->
        [{<<"deleted">>, true}];
    false ->
        []
    end},
    case IncludeDoc of
    true ->
        case Del of
        true ->
            DocVal = {<<"doc">>, null};
        false ->
            DocOptions = if Conflicts -> [conflicts]; true -> [] end,
            [DocVal] = couch_httpd_view:doc_member(Db, DocInfo, DocOptions),
            DocVal
        end,
        {{Id, Id}, Value, DocVal};
    false ->
        {{Id, Id}, Value}
    end.

make_map_fold_fun(false, _Conflicts, _Db) ->
    fun(Row, _, Acc) ->
        couch_index_iter:yield(Row)
        {ok, Acc}
    end;

make_map_fold_fun(true, Conflicts, Db) ->
    DocOpenOpts = if Conflicts -> [conflicts]; true -> [] end,
    fun({{_Key, error}, _Value} = Row, _, Acc) ->
        couch_index_iter:yield(Row),
        {ok, Acc};
    ({{_Key, DocId} = Kd, {Props} = Value}, _, Acc) ->
        IncludeId = get_value(<<"_id">>, Props, DocId),
        [Doc] = couch_httpd_view:doc_member(Db, IncludeId, DocOpenOpts),
        couch_index_iter:yield({Kd, Value, Doc}),
        {ok, Acc};
    ({{_Key, DocId} = Kd, Value}, _, Acc) ->
        [Doc] = couch_httpd_view:doc_member(Db, DocId, DocOpenOpts),
        couch_index_iter:yield({Kd, Value, Doc}),
        {ok, Acc}
    end.

reduce_view_folder(Db, ViewSpec, MergeParams, _UserCtx, DDoc) ->
    #simple_index_spec{
        ddoc_database = DDocDbName, ddoc_id = DDocId, index_name = ViewName
    } = ViewSpec,
    #index_merge{
        http_params = ViewArgs,
        extra = #view_merge{
            keys = Keys
        }
    } = MergeParams,
    #view_query_args{
        stale = Stale
    } = ViewArgs,
    FoldlFun = make_reduce_fold_fun(ViewArgs),
    KeyGroupFun = make_group_rows_fun(ViewArgs),
    {DDocDb, View} = get_reduce_view(Db, DDocDbName, DDocId, ViewName, Stale),

    case Keys of
    nil ->
        FoldOpts = [{key_group_fun, KeyGroupFun} |
            couch_httpd_view:make_key_options(ViewArgs)],
        {ok, _} = couch_view:fold_reduce(View, FoldlFun, [], FoldOpts);
    _ when is_list(Keys) ->
        lists:foreach(
            fun(K) ->
                FoldOpts = [{key_group_fun, KeyGroupFun} |
                    couch_httpd_view:make_key_options(
                        ViewArgs#view_query_args{
                            start_key = K, end_key = K})],
                {ok, _} = couch_view:fold_reduce(View, FoldlFun, [], FoldOpts)
            end,
            Keys)
    end,

    catch couch_db:close(DDocDb).

get_reduce_view(Db, DDocDbName, DDocId, ViewName, Stale) ->
    GroupId = couch_index_merger:get_group_id(DDocDbName, DDocId),
    {ok, View, _} = couch_view:get_reduce_view(Db, GroupId, ViewName, Stale),
    case GroupId of
        {DDocDb, DDocId} ->
            catch couch_db:close(DDocDb),
            View;
        DDocId ->
            View
    end.

view_type(nil, <<"_all_docs">>, _ViewArgs) ->
    map;
view_type(#doc{json = DDoc}, ViewName, ViewArgs) ->
    {Props} = DDoc,
    {ViewDef} = get_nested_json_value(DDoc, [<<"views">>, ViewName]),
    ViewType = case get_value(<<"reduce">>, ViewDef) of
    undefined ->
        map;
    RedFun when is_binary(RedFun) ->
        case ViewArgs#view_query_args.run_reduce of
        true ->
            reduce;
        false ->
            red_map
        end
    end.

folder(reduce, Db, ViewSpec, MergeParams, UserCtx, DDoc) ->
    reduce_view_folder(Db, ViewSpec, MergeParams, UserCtx, DDoc);
folder(Map, Db, ViewSpec, MergeParams, UserCtx, DDoc)
  when Map =:= map;
       Map =:= red_map ->
    map_view_folder(Db, ViewSpec, MergeParams, UserCtx, DDoc).

get_view(reduce, Db, DDocDbName, DDocId, ViewName, Stale) ->
    get_reduce_view(Db, DDocDbName, DDocId, ViewName, Stale);
get_view(Map, Db, DDocDbName, DDocId, ViewName, Stale)
  when Map =:= map;
       Map =:= red_map ->
    get_map_view(Db, DDocDbName, DDocId, ViewName, Stale).
