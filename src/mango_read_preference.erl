%% @hidden
-module(mango_read_preference).

-export([
    new/1,
    mode/1,
    mode/2,
    max_staleness_seconds/1,
    to_map/1
]).

-record(read_preference, {mode = primary, max_staleness_seconds = 90}).

-type t() :: #read_preference{}.

-spec new(Opts :: mango:start_opts()) -> t().
new(#{read_preference := #{mode := Mode, max_staleness_seconds := MaxStalenessSeconds}})
        when erlang:is_atom(Mode)
        andalso erlang:is_integer(MaxStalenessSeconds)
        andalso MaxStalenessSeconds >= 90 ->
    #read_preference{mode = Mode, max_staleness_seconds = MaxStalenessSeconds};
new(#{read_preference := #{mode := Mode}}) when erlang:is_atom(Mode) ->
    #read_preference{mode = Mode};
new(#{read_preference := Mode}) when erlang:is_atom(Mode) ->
    #read_preference{mode = Mode};
new(_) -> #read_preference{}.

-spec mode(ReadPreference :: t()) -> mango:read_preference_mode().
mode(#read_preference{mode = Mode}) ->
    Mode.

-spec mode(ReadPreference :: t(), Format :: raw | mongodb) -> mango:read_preference_mode() | binary().
mode(#read_preference{mode = primary_preferred}, mongodb) ->
    <<"primaryPreferred">>;
mode(#read_preference{mode = secondary_preferred}, mongodb) ->
    <<"secondaryPreferred">>;
mode(#read_preference{mode = Mode}, _) ->
    Mode.

-spec max_staleness_seconds(ReadPreference :: t()) -> pos_integer().
max_staleness_seconds(#read_preference{max_staleness_seconds = MaxStalenessSeconds}) ->
    MaxStalenessSeconds.

-spec to_map(ReadPreference :: t()) -> map().
to_map(#read_preference{max_staleness_seconds = MaxStalenessSeconds} = ReadPreference) ->
    #{<<"mode">> => mode(ReadPreference, mongodb), <<"maxStalenessSeconds">> => MaxStalenessSeconds}.
