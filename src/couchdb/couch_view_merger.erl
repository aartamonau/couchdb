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

-module(couch_view_merger).

% export callbacks
-export([parse_http_params/4, make_funs/3, get_skip_and_limit/1,
    http_index_folder_req_details/3, make_event_fun/2]).

-include("couch_db.hrl").
-include("couch_index_merger.hrl").
-include("couch_view_merger.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").

-define(LOCAL, <<"local">>).

-import(couch_util, [
    get_value/2,
    get_value/3,
    to_binary/1,
    get_nested_json_value/2
]).

-define(DEFAULT_STALENESS, update_after).


% callback!
parse_http_params(Req, DDoc, ViewName, #view_merge{keys = Keys}) ->
    % view type =~ query type
    {_Collation, ViewType0, _ViewLang} = view_details(DDoc, ViewName),
    ViewType = case {ViewType0, couch_httpd:qs_value(Req, "reduce", "true")} of
    {reduce, "false"} ->
       red_map;
    _ ->
       ViewType0
    end,

    StaleDefined = couch_httpd:qs_value(Req, "stale") =/= undefined,
    QueryArgs = couch_httpd_view:parse_view_params(Req, Keys, ViewType),
    QueryArgs1 = QueryArgs#view_query_args{view_name = ViewName},

    case StaleDefined of
    true ->
        QueryArgs1;
    false ->
        QueryArgs1#view_query_args{stale = ?DEFAULT_STALENESS}
    end.

% callback!
make_funs(DDoc, ViewName, IndexMergeParams) ->
    #index_merge{
       extra = Extra,
       http_params = #view_query_args{debug = DebugMode} = ViewArgs
    } = IndexMergeParams,
    #view_merge{
       rereduce_fun = InRedFun,
       rereduce_fun_lang = InRedFunLang,
       make_row_fun = MakeRowFun0
    } = Extra,
    {Collation, ViewType0, ViewLang} = view_details(DDoc, ViewName),
    ViewType = case {ViewType0, ViewArgs#view_query_args.run_reduce} of
    {reduce, false} ->
       red_map;
    _ ->
       ViewType0
    end,
    {RedFun, RedFunLang} = case {ViewType, InRedFun} of
    {reduce, nil} ->
        {reduce_function(DDoc, ViewName), ViewLang};
    {reduce, _} when is_binary(InRedFun) ->
        {InRedFun, InRedFunLang};
    _ ->
        {nil, nil}
    end,
    LessFun = view_less_fun(Collation, ViewArgs#view_query_args.direction,
        ViewType),
    {FoldFun, MergeFun} = case ViewType of
    reduce ->
        {fun reduce_view_folder/6, fun merge_reduce_views/1};
    _ when ViewType =:= map; ViewType =:= red_map ->
        {fun map_view_folder/6, fun merge_map_views/1}
    end,
    CollectorFun = case ViewType of
    reduce ->
        fun (_NumFolders, Callback2, UserAcc2) ->
            fun (Item) ->
                {ok, UserAcc3} = Callback2(start, UserAcc2),
                MakeRowFun = case is_function(MakeRowFun0) of
                true ->
                    MakeRowFun0;
                false ->
                    fun(RowDetails) -> view_row_obj_reduce(RowDetails, DebugMode) end
                end,
                couch_index_merger:collect_rows(MakeRowFun, Callback2, UserAcc3, Item)
            end
        end;
     % red_map|map
     _ ->
        fun (NumFolders, Callback2, UserAcc2) ->
            fun (Item) ->
                MakeRowFun = case is_function(MakeRowFun0) of
                true ->
                    MakeRowFun0;
                false ->
                    fun(RowDetails) -> view_row_obj_map(RowDetails, DebugMode) end
                end,
                couch_index_merger:collect_row_count(
                    NumFolders, 0, MakeRowFun, Callback2, UserAcc2, Item)
            end
        end
    end,
    Extra2 = #view_merge{
        rereduce_fun = RedFun,
        rereduce_fun_lang = RedFunLang
    },
    {LessFun, FoldFun, MergeFun, CollectorFun, Extra2}.

% callback!
get_skip_and_limit(#view_query_args{skip=Skip, limit=Limit}) ->
    {Skip, Limit}.

% callback!
make_event_fun(ViewArgs, Queue) ->
    fun(Ev) ->
        http_view_fold(Ev, ViewArgs#view_query_args.view_type, Queue)
    end.

% callback!
http_index_folder_req_details(#merged_index_spec{
        url = MergeUrl0, ejson_spec = {EJson}}, MergeParams, DDoc) ->
    #index_merge{
        conn_timeout = Timeout,
        http_params = ViewArgs,
        extra = #view_merge{
            keys = Keys
        }
    } = MergeParams,
    {ok, #httpdb{url = Url, ibrowse_options = Options} = Db} =
        couch_index_merger:open_db(MergeUrl0, nil, Timeout),
    MergeUrl = Url ++ view_qs(ViewArgs, MergeParams),
    Headers = [{"Content-Type", "application/json"} | Db#httpdb.headers],

    EJson1 = case Keys of
    nil ->
        EJson;
    _ ->
        [{<<"keys">>, Keys} | EJson]
    end,

    EJson2 = case couch_index_merger:should_check_rev(MergeParams, DDoc) of
    true ->
        P = fun (Tuple) -> element(1, Tuple) =/= <<"ddoc_revision">> end,
        [{<<"ddoc_revision">>, couch_index_merger:ddoc_rev_str(DDoc)} |
            lists:filter(P, EJson1)];
    false ->
        EJson1
    end,

    Body = {EJson2},
    put(from_url, Url),
    {MergeUrl, post, Headers, ?JSON_ENCODE(Body), Options}.

reduce_function(#doc{body = DDoc}, ViewName) ->
    {ViewDef} = get_nested_json_value(DDoc, [<<"views">>, ViewName]),
    get_value(<<"reduce">>, ViewDef).

view_less_fun(Collation, Dir, ViewType) ->
    LessFun = case Collation of
    <<"default">> ->
        case ViewType of
        _ when ViewType =:= map; ViewType =:= red_map ->
            fun(RowA, RowB) ->
                couch_view:less_json_ids(element(1, RowA), element(1, RowB))
            end;
        reduce ->
            fun({KeyA, _}, {KeyB, _}) -> couch_view:less_json(KeyA, KeyB) end
        end;
    <<"raw">> ->
        fun(A, B) -> A < B end
    end,
    case Dir of
    fwd ->
        LessFun;
    rev ->
        fun(A, B) -> not LessFun(A, B) end
    end.

view_row_obj_map({{Key, error}, Value}, _DebugMode) ->
    {[{key, Key}, {error, Value}]};

% set view
view_row_obj_map({{Key, DocId}, {PartId, Value}}, true) ->
    {[{id, DocId}, {key, Key}, {partition, PartId}, {value, Value}]};
view_row_obj_map({{Key, DocId}, {_PartId, Value}}, false) ->
    {[{id, DocId}, {key, Key}, {value, Value}]};

view_row_obj_map({{Key, DocId}, Value}, _DebugMode) ->
    {[{id, DocId}, {key, Key}, {value, Value}]};

% set view
view_row_obj_map({{Key, DocId}, {PartId, Value}, Doc}, true) ->
    {[{id, DocId}, {key, Key}, {partition, PartId}, {value, Value}, Doc]};
view_row_obj_map({{Key, DocId}, {_PartId, Value}, Doc}, false) ->
    {[{id, DocId}, {key, Key}, {value, Value}, Doc]};

view_row_obj_map({{Key, DocId}, Value, Doc}, _DebugMode) ->
    {[{id, DocId}, {key, Key}, {value, Value}, Doc]}.

view_row_obj_reduce({Key, Value}, _DebugMode) ->
    {[{key, Key}, {value, Value}]}.


merge_map_views(#merge_params{limit = 0} = Params) ->
    couch_index_merger:merge_indexes_no_limit(Params);

merge_map_views(#merge_params{row_acc = []} = Params) ->
    case couch_index_merger:merge_indexes_no_acc(
        Params, fun merge_map_min_row/2) of
    {params, Params2} ->
        merge_map_views(Params2);
    Else ->
        Else
    end;

merge_map_views(Params) ->
    Params2 = couch_index_merger:handle_skip(Params),
    merge_map_views(Params2).


% A new Params record is returned
merge_map_min_row(Params, MinRow) ->
    #merge_params{
        queue = Queue, index_name = ViewName
    } = Params,
    {RowToSend, RestToSend} = handle_duplicates(ViewName, MinRow, Queue),
    ok = couch_view_merger_queue:flush(Queue),
    couch_index_merger:handle_skip(
        Params#merge_params{row_acc=[RowToSend|RestToSend]}).



handle_duplicates(<<"_all_docs">>, MinRow, Queue) ->
    handle_all_docs_row(MinRow, Queue);

handle_duplicates(_ViewName, MinRow, Queue) ->
    handle_duplicates_allowed(MinRow, Queue).


handle_all_docs_row(MinRow, Queue) ->
    {Key0, Id0} = element(1, MinRow),
    % Group rows by similar keys, split error "not_found" from normal ones. If all
    % are "not_found" rows, squash them into one. If there are "not_found" ones
    % and others with a value, discard the "not_found" ones.
    {ValueRows, ErrorRows} = case Id0 of
    error ->
        pop_similar_rows(Key0, Queue, [], [MinRow]);
    _ when is_binary(Id0) ->
        pop_similar_rows(Key0, Queue, [MinRow], [])
    end,
    case {ValueRows, ErrorRows} of
    {[], [ErrRow | _]} ->
        {ErrRow, []};
    {[ValRow], []} ->
        {ValRow, []};
    {[FirstVal | RestVal], _} ->
        {FirstVal, RestVal}
    end.

handle_duplicates_allowed(MinRow, _Queue) ->
    {MinRow, []}.

pop_similar_rows(Key0, Queue, Acc, AccError) ->
    case couch_view_merger_queue:peek(Queue) of
    empty ->
        {Acc, AccError};
    {ok, Row} ->
        {Key, DocId} = element(1, Row),
        case Key =:= Key0 of
        false ->
            {Acc, AccError};
        true ->
            {ok, Row} = couch_view_merger_queue:pop_next(Queue),
            case DocId of
            error ->
                pop_similar_rows(Key0, Queue, Acc, [Row | AccError]);
            _ ->
                pop_similar_rows(Key0, Queue, [Row | Acc], AccError)
            end
        end
    end.


merge_reduce_views(#merge_params{limit = 0} = Params) ->
    couch_index_merger:merge_indexes_no_limit(Params);

merge_reduce_views(Params) ->
    case couch_index_merger:merge_indexes_no_acc(
        Params, fun merge_reduce_min_row/2) of
    {params, Params2} ->
        merge_reduce_views(Params2);
    Else ->
        Else
    end.

merge_reduce_min_row(Params, MinRow) ->
    #merge_params{
        queue = Queue, limit = Limit, skip = Skip, collector = Col
    } = Params,
    case group_keys_for_rereduce(Queue, [MinRow]) of
    revision_mismatch -> revision_mismatch;
    RowGroup ->
        ok = couch_view_merger_queue:flush(Queue),
        {Row, Col2} = case RowGroup of
        [R] ->
            {{row, R}, Col};
        [{K, _}, _ | _] ->
            try
                RedVal = rereduce(RowGroup, Params),
                {{row, {K, RedVal}}, Col}
            catch
            _Tag:Error ->
                Stack = erlang:get_stacktrace(),
                ?LOG_ERROR("Caught unexpected error while "
                           "merging reduce view: ~p~n~p", [Error, Stack]),
                on_rereduce_error(Col, Error)
            end
        end,
        case Row of
        {stop, _Resp} = Stop ->
            Stop;
        _ ->
            case Skip > 0 of
            true ->
                Limit2 = Limit,
                Col3 = Col2;
            false ->
                case Row of
                {row, _} ->
                    {ok, Col3} = Col2(Row);
                _ ->
                    Col3 = Col2,
                    ok
                end,
                Limit2 = couch_index_merger:dec_counter(Limit)
            end,
            Params#merge_params{
                skip = couch_index_merger:dec_counter(Skip), limit = Limit2,
                collector = Col3
            }
        end
    end.


on_rereduce_error(Col, Error) ->
    case Col(reduce_error(Error)) of
    {stop, _Resp} = Stop ->
            {Stop, undefined};
    Other ->
            Other
    end.

reduce_error({invalid_value, Reason}) ->
    {error, ?LOCAL, to_binary(Reason)};
reduce_error(Error) ->
    {error, ?LOCAL, to_binary(Error)}.


group_keys_for_rereduce(Queue, [{K, _} | _] = Acc) ->
    case couch_view_merger_queue:peek(Queue) of
    empty ->
        Acc;
    {ok, {K, _} = Row} ->
        {ok, Row} = couch_view_merger_queue:pop_next(Queue),
        group_keys_for_rereduce(Queue, [Row | Acc]);
    {ok, revision_mismatch} ->
        revision_mismatch;
    {ok, _} ->
        Acc
    end.


rereduce(Rows, #merge_params{extra = Extra}) ->
    #view_merge{
        rereduce_fun = RedFun,
        rereduce_fun_lang = Lang
    } = Extra,
    Reds = [[Val] || {_Key, Val} <- Rows],
    {ok, [Value]} = couch_query_servers:rereduce(Lang, [RedFun], Reds),
    Value.

http_view_fold(object_start, map, Queue) ->
    fun(Ev) -> http_view_fold_rc_1(Ev, Queue) end;
http_view_fold(object_start, red_map, Queue) ->
    fun(Ev) -> http_view_fold_rc_1(Ev, Queue) end;
http_view_fold(object_start, reduce, Queue) ->
    fun(Ev) -> http_view_fold_rows_1(Ev, Queue) end.

http_view_fold_rc_1({key, <<"total_rows">>}, Queue) ->
    fun(Ev) -> http_view_fold_rc_2(Ev, Queue) end;
http_view_fold_rc_1(_Ev, Queue) ->
    fun(Ev) -> http_view_fold_rc_1(Ev, Queue) end.

http_view_fold_rc_2(RowCount, Queue) when is_number(RowCount) ->
    ok = couch_view_merger_queue:queue(Queue, {row_count, RowCount}),
    fun(Ev) -> http_view_fold_rows_1(Ev, Queue) end.

http_view_fold_rows_1({key, <<"rows">>}, Queue) ->
    fun(array_start) -> fun(Ev) -> http_view_fold_rows_2(Ev, Queue) end end;
http_view_fold_rows_1(_Ev, Queue) ->
    fun(Ev) -> http_view_fold_rows_1(Ev, Queue) end.

http_view_fold_rows_2(array_end, Queue) ->
    fun(Ev) -> http_view_fold_extra(Ev, Queue) end;
http_view_fold_rows_2(object_start, Queue) ->
    fun(Ev) ->
        json_stream_parse:collect_object(
            Ev,
            fun(Row) ->
                http_view_fold_queue_row(Row, Queue),
                fun(Ev2) -> http_view_fold_rows_2(Ev2, Queue) end
            end)
    end.

http_view_fold_extra({key, <<"errors">>}, Queue) ->
    fun(array_start) -> fun(Ev) -> http_view_fold_errors(Ev, Queue) end end;
http_view_fold_extra({key, <<"debug_info">>}, Queue) ->
    fun(object_start) -> fun(Ev) -> http_view_fold_debug_info(Ev, Queue, []) end end;
http_view_fold_extra(_Ev, _Queue) ->
    fun couch_index_merger:void_event/1.

http_view_fold_errors(array_end, _Queue) ->
    fun couch_index_merger:void_event/1;
http_view_fold_errors(object_start, Queue) ->
    fun(Ev) ->
        json_stream_parse:collect_object(
            Ev,
            fun(Error) ->
                http_view_fold_queue_error(Error, Queue),
                fun(Ev2) -> http_view_fold_errors(Ev2, Queue) end
            end)
    end.

http_view_fold_debug_info({key, Key}, Queue, Acc) ->
    fun(object_start) ->
        fun(Ev) ->
            json_stream_parse:collect_object(
                Ev,
                fun(DebugInfo) ->
                    fun(Ev2) -> http_view_fold_debug_info(Ev2, Queue, [{Key, DebugInfo} | Acc]) end
                end)
        end
    end;
http_view_fold_debug_info(object_end, Queue, Acc) ->
    case Acc of
    [{?LOCAL, Info}] ->
        ok;
    _ ->
        Info = {lists:reverse(Acc)}
    end,
    ok = couch_view_merger_queue:queue(Queue, {debug_info, ?l2b(get(from_url)), Info}),
    fun(Ev2) -> http_view_fold_extra(Ev2, Queue) end.


http_view_fold_queue_error({Props}, Queue) ->
    From0 = get_value(<<"from">>, Props),
    From = case From0 of
    undefined ->
        get(from_url);
    _ ->
        From0
    end,
    Reason = get_value(<<"reason">>, Props, null),
    ok = couch_view_merger_queue:queue(Queue, {error, From, Reason}).

http_view_fold_queue_row({Props}, Queue) ->
    Key = get_value(<<"key">>, Props, null),
    Id = get_value(<<"id">>, Props, nil),
    Val = get_value(<<"value">>, Props),
    Value = case get_value(<<"partition">>, Props, nil) of
    nil ->
        Val;
    PartId ->
        {PartId, Val}
    end,
    Row = case get_value(<<"error">>, Props, nil) of
    nil ->
        case Id of
        nil ->
            % reduce row
            {Key, Val};
        _ ->
            % map row
            case get_value(<<"doc">>, Props, nil) of
            nil ->
                {{Key, Id}, Value};
            Doc ->
                {{Key, Id}, Value, {doc, Doc}}
            end
        end;
    Error ->
        % error in a map row
        {{Key, error}, Error}
    end,
    ok = couch_view_merger_queue:queue(Queue, Row).

make_group_rows_fun(#view_query_args{group_level = 0}) ->
    fun(_, _) -> true end;

make_group_rows_fun(#view_query_args{group_level = L}) when is_integer(L) ->
    fun({KeyA, _}, {KeyB, _}) when is_list(KeyA) andalso is_list(KeyB) ->
        lists:sublist(KeyA, L) == lists:sublist(KeyB, L);
    ({KeyA, _}, {KeyB, _}) ->
        KeyA == KeyB
    end;

make_group_rows_fun(_) ->
    fun({KeyA, _}, {KeyB, _}) -> KeyA == KeyB end.


make_reduce_fold_fun(#view_query_args{group_level = 0}, Queue) ->
    fun(_Key, Red, Acc) ->
        ok = couch_view_merger_queue:queue(Queue, {null, Red}),
        {ok, Acc}
    end;

make_reduce_fold_fun(#view_query_args{group_level = L}, Queue) when is_integer(L) ->
    fun(Key, Red, Acc) when is_list(Key) ->
        ok = couch_view_merger_queue:queue(Queue, {lists:sublist(Key, L), Red}),
        {ok, Acc};
    (Key, Red, Acc) ->
        ok = couch_view_merger_queue:queue(Queue, {Key, Red}),
        {ok, Acc}
    end;

make_reduce_fold_fun(_QueryArgs, Queue) ->
    fun(Key, Red, Acc) ->
        ok = couch_view_merger_queue:queue(Queue, {Key, Red}),
        {ok, Acc}
    end.

view_undefined_msg(SetName, DDocId) ->
    Msg = io_lib:format(
        "Undefined set view `~s` for `~s` design document.",
            [SetName, DDocId]),
    iolist_to_binary(Msg).

view_qs(ViewArgs, MergeParams) ->
    DefViewArgs = #view_query_args{},
    #view_query_args{
        start_key = StartKey, end_key = EndKey,
        start_docid = StartDocId, end_docid = EndDocId,
        direction = Dir,
        inclusive_end = IncEnd,
        group_level = GroupLevel,
        view_type = ViewType,
        include_docs = IncDocs,
        conflicts = Conflicts,
        stale = Stale,
        debug = Debug
    } = ViewArgs,
    #index_merge{on_error = OnError} = MergeParams,

    QsList = case StartKey =:= DefViewArgs#view_query_args.start_key of
    true ->
        [];
    false ->
        ["startkey=" ++ json_qs_val(StartKey)]
    end ++
    case EndKey =:= DefViewArgs#view_query_args.end_key of
    true ->
        [];
    false ->
        ["endkey=" ++ json_qs_val(EndKey)]
    end ++
    case {Dir, StartDocId =:= DefViewArgs#view_query_args.start_docid} of
    {fwd, false} ->
        ["startkey_docid=" ++ ?b2l(StartDocId)];
    _ ->
        []
    end ++
    case {Dir, EndDocId =:= DefViewArgs#view_query_args.end_docid} of
    {fwd, false} ->
        ["endkey_docid=" ++ ?b2l(EndDocId)];
    _ ->
        []
    end ++
    case Dir of
    fwd ->
        [];
    rev ->
        StartDocId1 = reverse_key_default(StartDocId),
        EndDocId1 = reverse_key_default(EndDocId),
        ["descending=true"] ++
        case StartDocId1 =:= DefViewArgs#view_query_args.start_docid of
        true ->
            [];
        false ->
            ["startkey_docid=" ++ json_qs_val(StartDocId1)]
        end ++
        case EndDocId1 =:= DefViewArgs#view_query_args.end_docid of
        true ->
            [];
        false ->
            ["endkey_docid=" ++ json_qs_val(EndDocId1)]
        end
    end ++
    case IncEnd =:= DefViewArgs#view_query_args.inclusive_end of
    true ->
        [];
    false ->
        ["inclusive_end=" ++ atom_to_list(IncEnd)]
    end ++
    case GroupLevel =:= DefViewArgs#view_query_args.group_level of
    true ->
        [];
    false ->
        case GroupLevel of
        exact ->
            ["group=true"];
        _ when is_number(GroupLevel) ->
            ["group_level=" ++ integer_to_list(GroupLevel)]
        end
    end ++
    case ViewType of
    red_map ->
        ["reduce=false"];
    _ ->
        []
    end ++
    case IncDocs =:= DefViewArgs#view_query_args.include_docs of
    true ->
        [];
    false ->
        ["include_docs=" ++ atom_to_list(IncDocs)]
    end ++
    case Conflicts =:= DefViewArgs#view_query_args.conflicts of
    true ->
        [];
    false ->
        ["conflicts=" ++ atom_to_list(Conflicts)]
    end ++
    %% we now have different default
    case Stale =:= ?DEFAULT_STALENESS of
    true ->
        [];
    false ->
        ["stale=" ++ atom_to_list(Stale)]
    end ++
    case OnError =:= ?ON_ERROR_DEFAULT of
    true ->
        [];
    false ->
        ["on_error=" ++ atom_to_list(OnError)]
    end ++
    case Debug =:= DefViewArgs#view_query_args.debug of
    true ->
        [];
    false ->
        ["debug=" ++ atom_to_list(Debug)]
    end,
    case QsList of
    [] ->
        [];
    _ ->
        "?" ++ string:join(QsList, "&")
    end.

json_qs_val(Value) ->
    couch_httpd:quote(?b2l(iolist_to_binary(?JSON_ENCODE(Value)))).

reverse_key_default(?MIN_STR) -> ?MAX_STR;
reverse_key_default(?MAX_STR) -> ?MIN_STR;
reverse_key_default(Key) -> Key.

view_details(nil, <<"_all_docs">>) ->
    {<<"raw">>, map, nil};

view_details(#doc{json = DDoc}, ViewName) ->
    {Props} = DDoc,
    {ViewDef} = get_nested_json_value(DDoc, [<<"views">>, ViewName]),
    {ViewOptions} = get_value(<<"options">>, ViewDef, {[]}),
    Collation = get_value(<<"collation">>, ViewOptions, <<"default">>),
    ViewType = case get_value(<<"reduce">>, ViewDef) of
    undefined ->
        map;
    RedFun when is_binary(RedFun) ->
        reduce
    end,
    Lang = get_value(<<"language">>, Props, <<"javascript">>),
    {Collation, ViewType, Lang}.
