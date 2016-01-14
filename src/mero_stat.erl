-module(mero_stat).

-author('Miriam Pena <miriam.pena@adroll.com>').

-export([
    noop/3,
    verbose_noop/3,
    log/2,
    log/3
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


log(Msg, Args) ->
    log({undefined, undefined, []}, Msg, Args).
log({_,_ ,Context}, Msg, Args) ->
    ClusterName = proplists:get_value(cluster_name, Context),
    Host = proplists:get_value(host, Context),
    error_logger:error_msg("Mero [~p] [~p] " ++ Msg,
        [ClusterName, Host] ++ Args).
