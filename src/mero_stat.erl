-module(mero_stat).

-author('Miriam Pena <miriam.pena@adroll.com>').

-export([
    noop/3,
    verbose_noop/3
]).

%%%===================================================================
%%% API
%%%===================================================================

-spec noop(Type :: gauge | spiral | histogram, KeyTags :: iolist(), Value :: integer()) ->
    no_return().
noop(_Type, _KeyAndTags, _Value) ->
    ok.

-spec verbose_noop(Type :: gauge | spiral | histogram, KeyTags :: iolist(), Value :: integer()) ->
    no_return().
verbose_noop(Type, KeyAndTags, Value) ->
    error_logger:info_report({stat, Type, KeyAndTags, Value}).
