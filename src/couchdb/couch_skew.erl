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

-module(couch_skew).

-export([new/1, size/1, in/2, out/1, min/1]).

-record(skew, {less_fun, tree}).
-define(null, []).

new(LessFun) ->
    #skew{less_fun=LessFun,
          tree=?null}.

size(#skew{tree=?null}) ->
    0;
size(#skew{tree={Sz, _, _, _}}) ->
    Sz.

in(X, #skew{tree=?null} = Skew) ->
    Skew#skew{tree={1, X, ?null, ?null}};
in(X, #skew{less_fun=LessFun, tree=A} = Skew) ->
    NewTree = merge(LessFun, {1, X, ?null, ?null}, A),
    Skew#skew{tree=NewTree}.

out(#skew{less_fun=LessFun, tree={_Sz, X, A, B}} = Skew) ->
    NewTree = merge(LessFun, A, B),
    {X, Skew#skew{tree=NewTree}}.

min(#skew{tree={_, X, _, _}}) ->
    X.

merge(_LessFun, A, ?null) ->
    A;
merge(_LessFun, ?null, B) ->
    B;
merge(LessFun, {_, Xa, _, _} = A, {_, Xb, _, _} = B) ->
    case LessFun(Xa, Xb) of
    true ->
        join(LessFun, A, B);
    false ->
        join(LessFun, B, A)
    end.

join(LessFun, {Sz1, X, A, B}, {Sz2, _, _, _} = C) ->
    {Sz1 + Sz2, X, B, merge(LessFun, A, C)}.
