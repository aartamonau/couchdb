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

-module(couch_index_merger).

-export([query_index/2, query_index/3]).

% Only needed for indexer implementation. Those functions should perhaps go into
% a utils module.
% The functions open_db/3, dec_counter/1, should_check_rev/2, get_ddoc/2 are
% also needed by this file
-export([open_db/3, collect_rows/4, collect_row_count/6,
    merge_indexes_no_acc/2, merge_indexes_no_limit/1, handle_skip/1,
    dec_counter/1, get_group_id/2, void_event/1, should_check_rev/2,
    ddoc_rev_str/1, ddoc_unchanged/2, validate_revision_param/1,
    rem_passwd/1, ddoc_not_found_msg/2]).

-include("couch_db.hrl").
-include("couch_index_merger.hrl").
% needed for #set_view_spec{}
-include("couch_view_merger.hrl").

-import(couch_util, [
    get_value/2,
    to_binary/1
]).

-define(LOCAL, <<"local">>).

-define(RETRY_INTERVAL, 1000).
-define(MAX_RETRIES, 30).


query_index(Mod, #index_merge{http_params = HttpParams, user_ctx = UserCtx} = IndexMergeParams) when HttpParams =/= nil, UserCtx =/= nil ->
    #index_merge{
        indexes = Indexes,
        user_ctx = UserCtx,
        conn_timeout = Timeout
    } = IndexMergeParams,
    {ok, DDoc, IndexName} = get_first_ddoc(Indexes, UserCtx, Timeout),
    query_index_loop(Mod, IndexMergeParams, DDoc, IndexName, ?MAX_RETRIES).

query_index(Mod, IndexMergeParams0, #httpd{user_ctx = UserCtx} = Req) ->
    #index_merge{
        indexes = Indexes,
        conn_timeout = Timeout,
        extra = Extra
    } = IndexMergeParams0,
    {ok, DDoc, IndexName} = get_first_ddoc(Indexes, UserCtx, Timeout),
    IndexMergeParams = IndexMergeParams0#index_merge{
        user_ctx = UserCtx,
        http_params = Mod:parse_http_params(Req, DDoc, IndexName, Extra)
    },
    query_index_loop(Mod, IndexMergeParams, DDoc, IndexName, ?MAX_RETRIES).


query_index_loop(_Mod, _IndexMergeParams, _DDoc, _IndexName, 0) ->
    throw({error, revision_sync_failed});
query_index_loop(Mod, IndexMergeParams, DDoc, IndexName, N) ->
    try
        do_query_index(Mod, IndexMergeParams, DDoc, IndexName)
    catch
    throw:retry ->
        timer:sleep(?RETRY_INTERVAL),
        #index_merge{
            indexes = Indexes,
            user_ctx = UserCtx,
            conn_timeout = Timeout
        } = IndexMergeParams,
        {ok, DDoc2, IndexName} = get_first_ddoc(Indexes, UserCtx, Timeout),
        query_index_loop(Mod, IndexMergeParams, DDoc2, IndexName, N - 1)
    end.


do_query_index(Mod, IndexMergeParams, DDoc, IndexName) ->
    #index_merge{
       indexes = Indexes, callback = Callback, user_acc = UserAcc,
       ddoc_revision = DesiredDDocRevision, user_ctx = UserCtx
    } = IndexMergeParams,

    ?LOG_DEBUG("Running a index merging for the following indexes: ~p", [Indexes]),

    DDocRev = ddoc_rev(DDoc),
    case should_check_rev(IndexMergeParams, DDoc) of
    true ->
        case DesiredDDocRevision of
        auto ->
            ok;
        DDocRev ->
            ok;
        _ ->
            throw({error, revision_mismatch})
        end;
    false ->
        ok
    end,

    {LessFun, FoldFun, MergeFun, CollectorFun, Extra2} = Mod:make_funs(
        DDoc, IndexName, IndexMergeParams),
    NumFolders = length(Indexes),
    QueueLessFun = fun
        (set_view_outdated, _) ->
            true;
        (_, set_view_outdated) ->
            false;
        (revision_mismatch, _) ->
            true;
        (_, revision_mismatch) ->
            false;
        ({debug_info, _Url, _Info}, _) ->
            true;
        (_, {debug_info, _Url, _Info}) ->
            false;
        ({row_count, _}, _) ->
            true;
        (_, {row_count, _}) ->
            false;
        ({error, _Url, _Reason}, _) ->
            true;
        (_, {error, _Url, _Reason}) ->
            false;
        (RowA, RowB) ->
            case LessFun of
            nil ->
                % That's where the actual less fun is. But as bounding box
                % requests don't return a sorted order, we just return true
                true;
             _ ->
                LessFun(RowA, RowB)
            end
    end,
    % We want to trap exits to avoid this process (mochiweb worker) to die.
    % If the mochiweb worker dies, the client will not get a response back.
    % Link the queue to the folders, so that if one folder dies, all the others
    % will be killed and not hang forever (mochiweb reuses workers for different
    % requests).
    TrapExitBefore = process_flag(trap_exit, true),
    {ok, Queue} = couch_view_merger_queue:start_link(NumFolders, QueueLessFun),
    Collector = CollectorFun(NumFolders, Callback, UserAcc),
    Folders = lists:foldr(
        fun(Index, Acc) ->
            Pid = spawn_link(fun() ->
                link(Queue),
                index_folder(Mod, Index, IndexMergeParams, UserCtx, DDoc, Queue, FoldFun)
            end),
            [Pid | Acc]
        end,
        [], Indexes),
    {Skip, Limit} = Mod:get_skip_and_limit(IndexMergeParams#index_merge.http_params),
    MergeParams = #merge_params{
        index_name = IndexName,
        queue = Queue,
        collector = Collector,
        skip = Skip,
        limit = Limit,
        extra = Extra2
    },
    try
        case MergeFun(MergeParams) of
        set_view_outdated ->
            throw({error, set_view_outdated});
        revision_mismatch ->
            case DesiredDDocRevision of
            auto ->
                throw(retry);
            _ ->
                throw({error, revision_mismatch})
            end;
        {ok, Resp} ->
            Resp;
        {stop, Resp} ->
            Resp
        end
    after
        catch unlink(Queue),
        lists:foreach(
            fun (P) ->
                catch unlink(P),
                catch exit(P, kill)
            end, Folders),
        catch exit(Queue, kill),
        Reason = clean_exit_messages(normal),
        process_flag(trap_exit, TrapExitBefore),
        case Reason of
        normal ->
            ok;
        _ ->
            exit(Reason)
        end
    end.


clean_exit_messages(FinalReason) ->
    receive
    {'EXIT', _Pid, normal} ->
        clean_exit_messages(FinalReason);
    {'EXIT', _Pid, Reason} ->
        clean_exit_messages(Reason)
    after 0 ->
        FinalReason
    end.


get_first_ddoc([], _UserCtx, _Timeout) ->
    throw({error, <<"A view spec can not consist of merges exclusively.">>});

get_first_ddoc([#simple_index_spec{ddoc_id = nil} = Spec | _],
        _UserCtx, _Timeout) ->
    #simple_index_spec{index_name = <<"_all_docs">>} = Spec,
    {ok, nil, <<"_all_docs">>};

get_first_ddoc([#set_view_spec{} = Spec | _], UserCtx, Timeout) ->
    #set_view_spec {
        name = SetName, ddoc_id = Id, view_name = ViewName
    } = Spec,

    {ok, Db} = open_db(<<SetName/binary, "/master">>, UserCtx, Timeout),
    {ok, DDoc} = get_ddoc(Db, Id),
    close_db(Db),

    {ok, DDoc, ViewName};

get_first_ddoc([#simple_index_spec{} = Spec | _], UserCtx, Timeout) ->
    #simple_index_spec{
        database = DbName, ddoc_database = DDocDbName, ddoc_id = Id,
        index_name = IndexName
    } = Spec,
    {ok, Db} = case DDocDbName of
    nil ->
        open_db(DbName, UserCtx, Timeout);
    _ when is_binary(DDocDbName) ->
        open_db(DDocDbName, UserCtx, Timeout)
    end,
    {ok, DDoc} = get_ddoc(Db, Id),
    close_db(Db),

    {ok, DDoc, IndexName};

get_first_ddoc([_MergeSpec | Rest], UserCtx, Timeout) ->
    get_first_ddoc(Rest, UserCtx, Timeout).


open_db(<<"http://", _/binary>> = DbName, _UserCtx, Timeout) ->
    HttpDb = #httpdb{
        url = maybe_add_trailing_slash(DbName),
        timeout = Timeout
    },
    {ok, HttpDb#httpdb{ibrowse_options = ibrowse_options(HttpDb)}};
open_db(<<"https://", _/binary>> = DbName, _UserCtx, Timeout) ->
    HttpDb = #httpdb{
        url = maybe_add_trailing_slash(DbName),
        timeout = Timeout
    },
    {ok, HttpDb#httpdb{ibrowse_options = ibrowse_options(HttpDb)}};
open_db(DbName, UserCtx, _Timeout) ->
    case couch_db:open(DbName, [{user_ctx, UserCtx}]) of
    {ok, _} = Ok ->
        Ok;
    {not_found, _} ->
        throw({not_found, db_not_found_msg(DbName)});
    Error ->
        throw(Error)
    end.

maybe_add_trailing_slash(Url) when is_binary(Url) ->
    maybe_add_trailing_slash(?b2l(Url));
maybe_add_trailing_slash(Url) ->
    case lists:last(Url) of
    $/ ->
        Url;
    _ ->
        Url ++ "/"
    end.

close_db(#httpdb{}) ->
    ok;
close_db(Db) ->
    couch_db:close(Db).

get_ddoc(Db, Id) ->
    case couch_db:open_doc(Db, Id, [ejson_body]) of
    {ok, _} = Ok ->
        Ok;
    {not_found, _} ->
        throw({not_found, ddoc_not_found_msg(Db, Id)})
    end.

% Returns the group ID of the indexer group that contains the Design Document
% In Couchbase the Design Document is stored in a so-called master database.
% This is Couchbase specific
get_group_id(nil, DDocId) ->
    DDocId;
get_group_id(DDocDbName, DDocId) when is_binary(DDocDbName) ->
    DDocDb = case couch_db:open_int(DDocDbName, []) of
    {ok, DDocDb1} ->
        DDocDb1;
    {not_found, _} ->
        throw(ddoc_db_not_found)
    end,
    {DDocDb, DDocId}.

db_uri(#db{name = Name}) ->
    Name;
db_uri(Url) when is_binary(Url) ->
    ?l2b(couch_util:url_strip_password(Url)).


db_not_found_msg(DbName) ->
    iolist_to_binary(io_lib:format(
        "Database `~s` doesn't exist.", [db_uri(DbName)])).

ddoc_not_found_msg(DbName, DDocId) ->
    Msg = io_lib:format(
        "Design document `~s` missing in database `~s`.",
        [DDocId, db_uri(DbName)]),
    iolist_to_binary(Msg).

ibrowse_error_msg(Reason) when is_atom(Reason) ->
    to_binary(Reason);
ibrowse_error_msg(Reason) when is_tuple(Reason) ->
    to_binary(element(1, Reason)).

ibrowse_options(#httpdb{timeout = T, url = Url}) ->
    [{inactivity_timeout, T}, {connect_timeout, infinity},
        {response_format, binary}, {socket_options, [{keepalive, true}]}] ++
    case Url of
    "https://" ++ _ ->
        % TODO: add SSL options like verify and cacertfile
        [{is_ssl, true}];
    _ ->
        []
    end.


collect_row_count(RecvCount, AccCount, PreprocessFun, Callback, UserAcc, Item) ->
    case Item of
    {error, _DbUrl, _Reason} = Error ->
        case Callback(Error, UserAcc) of
        {stop, Resp} ->
            {stop, Resp};
        {ok, UserAcc2} ->
            case RecvCount > 1 of
            false ->
                {ok, UserAcc3} = Callback({start, AccCount}, UserAcc2),
                {ok, fun (Item2) ->
                    collect_rows(
                        PreprocessFun, Callback, UserAcc3, Item2)
                end};
            true ->
                {ok, fun (Item2) ->
                    collect_row_count(
                        RecvCount - 1, AccCount, PreprocessFun, Callback,
                        UserAcc2, Item2)
                end}
            end
        end;
    {row_count, Count} ->
        AccCount2 = AccCount + Count,
        case RecvCount > 1 of
        false ->
            % TODO: what about offset and update_seq?
            % TODO: maybe add etag like for regular views? How to
            %       compute them?
            {ok, UserAcc2} = Callback({start, AccCount2}, UserAcc),
            {ok, fun (Item2) ->
                collect_rows(PreprocessFun, Callback, UserAcc2, Item2)
            end};
        true ->
            {ok, fun (Item2) ->
                collect_row_count(
                    RecvCount - 1, AccCount2, PreprocessFun, Callback, UserAcc, Item2)
            end}
        end;
    {debug_info, _From, _Info} = DebugInfo ->
        {ok, UserAcc2} = Callback(DebugInfo, UserAcc),
        {ok, fun (Item2) ->
            collect_row_count(RecvCount, AccCount, PreprocessFun, Callback, UserAcc2, Item2)
        end};
    stop ->
        {_, UserAcc2} = Callback(stop, UserAcc),
        {stop, UserAcc2}
    end.

% PreprocessFun is called on every row (which comes from the fold function
% of the underlying data structure) before it gets passed into the Callback
% function
collect_rows(PreprocessFun, Callback, UserAcc, Item) ->
    case Item of
    {error, _DbUrl, _Reason} = Error ->
        case Callback(Error, UserAcc) of
        {stop, Resp} ->
            {stop, Resp};
        {ok, UserAcc2} ->
            {ok, fun (Item2) ->
                collect_rows(PreprocessFun, Callback, UserAcc2, Item2)
            end}
        end;
    {row, Row} ->
        RowEJson = PreprocessFun(Row),
        {ok, UserAcc2} = Callback({row, RowEJson}, UserAcc),
        {ok, fun (Item2) ->
            collect_rows(PreprocessFun, Callback, UserAcc2, Item2)
        end};
    {debug_info, _From, _Info} = DebugInfo ->
        {ok, UserAcc2} = Callback(DebugInfo, UserAcc),
        {ok, fun (Item2) ->
            collect_rows(PreprocessFun, Callback, UserAcc2, Item2)
        end};
    stop ->
        {ok, UserAcc2} = Callback(stop, UserAcc),
        {stop, UserAcc2}
    end.

% When no limit is specified the merging is easy
merge_indexes_no_limit(#merge_params{collector = Col}) ->
    Col(stop).

% Simple case when there are no (or we don't care about) accumulated rows
% MinRowFun is a function that it called if the
% couch_view_merger_queue returns a row that is neither an error, nor a count.
merge_indexes_no_acc(Params, MinRowFun) ->
    #merge_params{
        queue = Queue, collector = Col
    } = Params,
    case couch_view_merger_queue:pop(Queue) of
    closed ->
        {stop, Resp} = Col(stop),
        {ok, Resp};
    {ok, {debug_info, _From, _Info} = DebugInfo} ->
        ok = couch_view_merger_queue:flush(Queue),
        {ok, Col2} = Col(DebugInfo),
        merge_indexes_no_acc(Params#merge_params{collector = Col2}, MinRowFun);
    {ok, revision_mismatch} ->
        revision_mismatch;
    {ok, set_view_outdated} ->
        set_view_outdated;
    {ok, {error, _Url, _Reason} = Error} ->
        ok = couch_view_merger_queue:flush(Queue),
        case Col(Error) of
        {ok, Col2} ->
            merge_indexes_no_acc(
                Params#merge_params{collector = Col2}, MinRowFun);
        {stop, Resp} ->
            {stop, Resp}
        end;
    {ok, {row_count, _} = RowCount} ->
        {ok, Col2} = Col(RowCount),
        ok = couch_view_merger_queue:flush(Queue),
        merge_indexes_no_acc(Params#merge_params{collector = Col2}, MinRowFun);
    {ok, MinRow} ->
        Params2 = MinRowFun(Params, MinRow),
        {params, Params2}
    end.

handle_skip(Params) ->
    #merge_params{
        limit = Limit, skip = Skip, collector = Col,
        row_acc = [RowToSend | Rest]
    } = Params,
    case Skip > 0 of
    true ->
        Limit2 = Limit,
        Col2 = Col;
    false ->
        {ok, Col2} = Col({row, RowToSend}),
        Limit2 = dec_counter(Limit)
    end,
    Params#merge_params{
        skip = dec_counter(Skip), limit = Limit2, row_acc = Rest,
        collector = Col2
    }.

dec_counter(0) -> 0;
dec_counter(N) -> N - 1.


index_folder(Mod, #merged_index_spec{} = IndexSpec,
        MergeParams, _UserCtx, DDoc, Queue, _FoldFun) ->
    http_index_folder(Mod, IndexSpec, MergeParams, DDoc, Queue);

index_folder(_Mod, #set_view_spec{} = ViewSpec, MergeParams,
        UserCtx, DDoc, Queue, FoldFun) ->
    FoldFun(nil, ViewSpec, MergeParams, UserCtx, DDoc, Queue);

index_folder(_Mod, IndexSpec, MergeParams, UserCtx, DDoc, Queue, FoldFun) ->
    #simple_index_spec{
        database = DbName, ddoc_database = DDocDbName, ddoc_id = DDocId
    } = IndexSpec,
    case couch_db:open(DbName, [{user_ctx, UserCtx}]) of
    {ok, Db} ->
        try
            FoldFun(Db, IndexSpec, MergeParams, UserCtx, DDoc, Queue)
        catch
        {not_found, Reason} when Reason =:= missing; Reason =:= deleted ->
            ok = couch_view_merger_queue:queue(
                Queue, {error, ?LOCAL, ddoc_not_found_msg(DbName, DDocId)});
        ddoc_db_not_found ->
            ok = couch_view_merger_queue:queue(
                Queue, {error, ?LOCAL, ddoc_not_found_msg(DDocDbName, DDocId)});
        _Tag:Error ->
            Stack = erlang:get_stacktrace(),
            ?LOG_ERROR("Caught unexpected error while serving "
                       "index query for `~s/~s`:~n~p", [DbName, DDocId, Stack]),
            couch_view_merger_queue:queue(Queue, parse_error(Error))
        after
            ok = couch_view_merger_queue:done(Queue),
            couch_db:close(Db)
        end;
    {not_found, _} ->
        ok = couch_view_merger_queue:queue(
            Queue, {error, ?LOCAL, db_not_found_msg(DbName)}),
        ok = couch_view_merger_queue:done(Queue)
    end.

% `invalid_value` only happens on reduces
parse_error({invalid_value, Reason}) ->
    {error, ?LOCAL, to_binary(Reason)};
parse_error(Error) ->
    {error, ?LOCAL, to_binary(Error)}.

% Fold function for remote indexes
http_index_folder(Mod, IndexSpec, MergeParams, DDoc, Queue) ->
    EventFun = Mod:make_event_fun(MergeParams#index_merge.http_params, Queue),
    {Url, Method, Headers, Body, Options} = Mod:http_index_folder_req_details(
        IndexSpec, MergeParams, DDoc),
    {ok, Conn} = ibrowse:spawn_link_worker_process(Url),

    #index_merge{
        conn_timeout = Timeout
    } = MergeParams,

    R = ibrowse:send_req_direct(
            Conn, Url, Headers, Method, Body,
            [{stream_to, {self(), once}} | Options], Timeout),

    case R of
    {error, Reason} ->
        ok = couch_view_merger_queue:queue(Queue,
            {error, Url, ibrowse_error_msg(Reason)}),
        ok = couch_view_merger_queue:done(Queue);
    {ibrowse_req_id, ReqId} ->
        receive
        {ibrowse_async_headers, ReqId, "200", _RespHeaders} ->
            ibrowse:stream_next(ReqId),
            DataFun = fun() -> stream_data(ReqId) end,
            try
                json_stream_parse:events(DataFun, EventFun)
            catch throw:{error, Error} ->
                ok = couch_view_merger_queue:queue(Queue, {error, Url, Error})
            after
                stop_conn(Conn),
                ok = couch_view_merger_queue:done(Queue)
            end;
        {ibrowse_async_headers, ReqId, Code, _RespHeaders} ->
            Error = try
                stream_all(ReqId, [])
            catch throw:{error, _Error} ->
                <<"Error code ", (?l2b(Code))/binary>>
            end,
            case (catch ?JSON_DECODE(Error)) of
            {Props} when is_list(Props) ->
                case {get_value(<<"error">>, Props),
                    get_value(<<"reason">>, Props)} of
                {<<"not_found">>, Reason} when
                        Reason =/= <<"missing">>, Reason =/= <<"deleted">> ->
                    ok = couch_view_merger_queue:queue(
                        Queue, {error, Url, Reason});
                {<<"not_found">>, _} ->
                    ok = couch_view_merger_queue:queue(
                        Queue, {error, Url, <<"not_found">>});
                {<<"error">>, <<"revision_mismatch">>} ->
                    ok = couch_view_merger_queue:queue(Queue, revision_mismatch);
                {<<"error">>, <<"set_view_outdated">>} ->
                    ?LOG_DEBUG("Got `set_view_outdated` from ~s", [Url]),
                    ok = couch_view_merger_queue:queue(Queue, set_view_outdated);
                JsonError ->
                    ok = couch_view_merger_queue:queue(
                        Queue, {error, Url, to_binary(JsonError)})
                end;
            _ ->
                ok = couch_view_merger_queue:queue(
                    Queue, {error, Url, to_binary(Error)})
            end,
            ok = couch_view_merger_queue:done(Queue),
            stop_conn(Conn);
        {ibrowse_async_response, ReqId, {error, Error}} ->
            stop_conn(Conn),
            ok = couch_view_merger_queue:queue(Queue, {error, Url, Error}),
            ok = couch_view_merger_queue:done(Queue)
        end
    end.


stop_conn(Conn) ->
    unlink(Conn),
    receive {'EXIT', Conn, _} -> ok after 0 -> ok end,
    catch ibrowse:stop_worker_process(Conn).


stream_data(ReqId) ->
    receive
    {ibrowse_async_response, ReqId, {error, _} = Error} ->
        throw(Error);
    {ibrowse_async_response, ReqId, <<>>} ->
        ibrowse:stream_next(ReqId),
        stream_data(ReqId);
    {ibrowse_async_response, ReqId, Data} ->
        ibrowse:stream_next(ReqId),
        {Data, fun() -> stream_data(ReqId) end};
    {ibrowse_async_response_end, ReqId} ->
        {<<>>, fun() -> throw({error, <<"more view data expected">>}) end}
    end.


stream_all(ReqId, Acc) ->
    case stream_data(ReqId) of
    {<<>>, _} ->
        iolist_to_binary(lists:reverse(Acc));
    {Data, _} ->
        stream_all(ReqId, [Data | Acc])
    end.

void_event(_Ev) ->
    fun void_event/1.

ddoc_rev(nil) ->
    nil;
ddoc_rev(#doc{rev = Rev}) ->
    Rev.

ddoc_rev_str(DDoc) ->
    couch_doc:rev_to_str(ddoc_rev(DDoc)).

should_check_rev(#index_merge{ddoc_revision = DDocRevision}, DDoc) ->
    DDocRevision =/= nil andalso DDoc =/= nil.

ddoc_unchanged(DbName, DDoc) when is_binary(DbName) ->
    case couch_db:open_int(DbName, []) of
    {ok, Db} ->
        try
            DDocId = DDoc#doc.id,
            {ok, MaybeUpdatedDDoc} = get_ddoc(Db, DDocId),
            ddoc_rev(DDoc) =:= ddoc_rev(MaybeUpdatedDDoc)
        after
            couch_db:close(Db)
        end;
    {not_found, _} ->
        throw(ddoc_db_not_found)
    end;
ddoc_unchanged(Db, DDoc) ->
    DbName = couch_db:name(Db),
    case couch_db:open_int(DbName, []) of
    {ok, Db1} ->
        try
            case couch_db:get_update_seq(Db) =:= couch_db:get_update_seq(Db1) of
            true ->
                %% nothing changed
                true;
            false ->
                %% design document may have changed
                DDocId = DDoc#doc.id,
                {ok, MaybeUpdatedDDoc} = get_ddoc(Db1, DDocId),
                ddoc_rev(DDoc) =:= ddoc_rev(MaybeUpdatedDDoc)
            end
        after
            couch_db:close(Db1)
        end;
    {not_found, _} ->
        throw(ddoc_db_not_found)
    end.

validate_revision_param(nil) ->
    nil;
validate_revision_param(<<"auto">>) ->
    auto;
validate_revision_param(Revision) ->
    couch_doc:parse_rev(Revision).

rem_passwd(Url) ->
    ?l2b(couch_util:url_strip_password(Url)).
