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

-module(couch_index_iter).

-export([new/3, await/1, stop/1, init_ack/1, yield/1, done/0]).

-record(iter, {worker, ref, done}).

new(M, F, A) ->
    R = proc_lib:start_link(
          fun () ->
              WorkerRef = make_ref(),
              put(worker_ref, WorkerRef),
              M:F(A)
          end),

    case R of
    {ok, Worker, WorkerRef} ->
         #iter{worker=Worker, ref=WorkerRef};
    {Error, Worker, _} ->
         do_stop(Worker),
         process_init_error(Error)
    end.

await(It#iter{done=true}) ->
    {done, It};
await(It#iter{worker=Worker, done=false}) ->
    Result = iter_get(Worker),
    process_next_result(Result, It).

stop(It#iter{worker=Worker}) ->
    do_stop(Worker).

do_stop(Worker) ->
    catch unlink(Worker),
    catch exit(Worker, kill).

init_ack(Term) ->
    undefined = get(worker_initialized),
    put(worker_initialized, true),

    proc_lib:init_ack({Term, self(), worker_ref()}).

yield(Term) ->
    receive
    {get, Pid, Ref} ->
         Pid ! {Ref, Term}
    end.

iter_get(Worker) ->
    Ref = make_ref(),
    Worker ! {get, self(), Ref},

    receive
    {Ref, Term} ->
         Term
    end.

done() ->
    yield({worker_ref(), done}).

worker_ref() ->
    WorkerRef = get(worker_ref),
    true = WorkerRef =/= undefined,
    WorkerRef.

process_init_error({error, Reason})
  when Reason =:= revision_mismatch;
       Reason =:= index_outdated ->
    throw(Reason);
process_init_error({error, _} = Other) ->
    Other.

process_next_result({WorkerRef, done},
                    It#iter{ref=WorkerRef}) ->
    {done, It#iter{done=true}};
process_next_result(Result, It) ->
    {{next, Result}, It}.
