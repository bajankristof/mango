%% @hidden
-module(mango_backoff).

-export([
    from_start_opts/1,
    new/2,
    apply/2,
    time/1,
    sleep/1,
    reset/1,
    n/1
]).

-include("./_defaults.hrl").

-record(backoff, {n = 0, min, max}).

-type t() :: #backoff{}.

-spec from_start_opts(Opts :: mango:start_opts()) -> t().
from_start_opts(#{} = Opts) ->
    Min = maps:get(min_backoff, Opts, ?DEFAULT_MIN_BACKOFF),
    Max = maps:get(max_backoff, Opts, ?DEFAULT_MAX_BACKOFF),
    new(Min, Max).

-spec new(Min :: pos_integer(), Max :: pos_integer()) -> t().
new(Min, Max) ->
    #backoff{min = Min, max = Max}.

-spec apply(Backoff :: t(), Mfa :: mfa()) -> {{ok | error, term()}, #backoff{}}.
apply(#backoff{} = Backoff0, {Module, Function, Args}) ->
    case erlang:apply(Module, Function, Args) of
        {ok, Result} ->
            {{ok, Result}, reset(Backoff0)};
        {error, Reason} ->
            Backoff = incr(Backoff0),
            {{error, Reason}, n(Backoff), Backoff}
    end.

-spec time(Backoff :: t()) -> pos_integer().
time(#backoff{n = N, min = Min, max = Max}) ->
    Time = Min * math:pow(2, N),
    erlang:min(Time, Max).

-spec sleep(Backoff :: t()) -> ok.
sleep(#backoff{} = Backoff) ->
    timer:sleep(time(Backoff)).

-spec incr(Backoff :: t()) -> #backoff{}.
incr(Backoff = #backoff{n = N}) ->
    Backoff#backoff{n = N + 1}.

-spec reset(Backoff :: t()) -> #backoff{}.
reset(Backoff = #backoff{}) ->
    Backoff#backoff{n = 0}.

-spec n(Backoff :: t()) -> non_neg_integer().
n(#backoff{n = N}) -> N.
