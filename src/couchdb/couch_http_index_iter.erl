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

-module(couch_http_index_iter).

-export([new/6, next/1]).

new(Url, Headers, Body, Options, Timeout, Fn) ->
    couch_index_iter:new(?MODULE, worker,
                         [Url, Headers, Body, Options, Timeout, Fn]).

worker(Parent, Url, Headers, Body, Options, Timeout, UserEventFun) ->
    {ok, Connection} = ibrowse:spawn_link_worker_process(Url),

    R = ibrowse:send_req_direct(
            Connection, Url, Headers, Method, Body,
            [{stream_to, {self(), once}} | Options], Timeout),

    case R of
    {ibrowse_req_id, ReqId} ->
        receive
        {ibrowse_async_headers, ReqId, "200", _RespHeaders} ->
            couch_index_iter:init_ack(ok),

            DataFun =
                fun () ->
                    stream_data(ReqId)
                end,

            EventFun =
                fun (Ev) ->
                    UserEventFun(Ev)
                end,

            try
                json_stream_parse:events(DataFun, EventFun)
            catch throw:{error, Error} ->
                couch_index_iter:yield({error, Error})
            end;
        {ibrowse_async_headers, ReqId, Code, _RespHeaders} ->
            Error = try
                stream_all(ReqId, [])
            catch throw:{error, _Error} ->
                <<"Error code ", (?l2b(Code))/binary>>
            end,

            couch_index_iter:yield({error, decode_common_errors(Rest)});
        {ibrowse_async_response, ReqId, {error, Error}} ->
            couch_index_iter:yield({error, Error})
        end;
    {error, Reason} ->
            couch_index_iter:yield({error, ibrowse_error_msg(Reason)})
    end,

    stop_connection(Connection),
    couch_index_iter:done().

decode_common_errors(Body) ->
    case (catch ?JSON_DECODE(Body)) of
    {Props} when is_list(Props) ->
        case {get_value(<<"error">>, Props),
            get_value(<<"reason">>, Props)} of
        {<<"not_found">>, Reason} when
                Reason =/= <<"missing">>, Reason =/= <<"deleted">> ->
            Reason;
        {<<"not_found">>, _} ->
            <<"not_found">>;
        {<<"error">>, <<"revision_mismatch">>} ->
            revision_mismatch;
        {<<"error">>, <<"set_view_outdated">>} ->
            set_view_outdated;
        JsonError ->
            JsonError;
        end;
    _ ->
        couch_util:to_binary(Body)
    end.

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

ibrowse_error_msg(Reason) when is_atom(Reason) ->
    to_binary(Reason);
ibrowse_error_msg(Reason) when is_tuple(Reason) ->
    to_binary(element(1, Reason)).

stop_connection(Connection) ->
    ibrowse:stop_worker_process(Connection).
