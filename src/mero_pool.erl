%% Copyright (c) 2014, AdRoll
%% All rights reserved.
%%
%% Redistribution and use in source and binary forms, with or without
%% modification, are permitted provided that the following conditions are met:
%%
%% * Redistributions of source code must retain the above copyright notice, this
%% list of conditions and the following disclaimer.
%%
%% * Redistributions in binary form must reproduce the above copyright notice,
%% this list of conditions and the following disclaimer in the documentation
%% and/or other materials provided with the distribution.
%%
%% * Neither the name of the {organization} nor the names of its
%% contributors may be used to endorse or promote products derived from
%% this software without specific prior written permission.
%%
%% THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
%% AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
%% IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
%% DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
%% FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
%% DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
%% SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
%% CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
%% OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
%% OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
%%
-module(mero_pool).

-author('Miriam Pena <miriam.pena@adroll.com>').

-export([start_link/5,
         checkout/2,
         checkin/1,
         checkin_closed/1,
         transaction/3,
         close/2,
         pool_loop/3,
         system_continue/3,
         system_terminate/4]).

%%% Internal & introspection functions
-export([init/6,
         state/1]).

-include_lib("mero/include/mero.hrl").

-record(conn, {updated :: erlang:timestamp(),
               pool :: module(),
               worker_module :: module(),
               client :: term()}).

-record(pool_st, {cluster,
                  host,
                  port,
                  max_connections,
                  min_connections,

                  %% List of free connections
                  free :: list(term()),

                  %% Busy connections (pid -> #conn)
                  busy :: dict:dict(),

                  %% Number of connections established (busy + free)
                  num_connected :: non_neg_integer(),

                  %% Number of connection attempts in progress
                  num_connecting :: non_neg_integer(),

                  %% Number of failed connection attempts
                  %% (reset to zero when connect attempt succeds)
                  num_failed_connecting :: non_neg_integer(),

                  reconnect_wait_time :: non_neg_integer(),
                  worker_module :: atom(),
                  stats_context :: {module(), Function :: atom()},
                  pool :: term()}).

%%%=============================================================================
%%% External functions
%%%=============================================================================

start_link(ClusterName, Host, Port, PoolName, WorkerModule) ->
    proc_lib:start_link(?MODULE, init, [self(), ClusterName, Host, Port, PoolName, WorkerModule]).

%% @doc Checks out an element of the pool.
-spec checkout(atom(), TimeLimit :: tuple()) ->
                      {ok, #conn{}} | {error, Reason :: term()}.
checkout(PoolName, TimeLimit) ->
    Timeout = mero_conf:millis_to(TimeLimit),
    MRef = erlang:monitor(process, PoolName),
    safe_send(PoolName, {checkout, {self(), MRef}}),
    receive
        {'DOWN', MRef, _, _, _} ->
            {error, down};
        {MRef, {reject, _State}} ->
            erlang:demonitor(MRef),
            {error, reject};
        {MRef, Connection} ->
            erlang:demonitor(MRef),
            {ok, Connection}
    after Timeout ->
            erlang:demonitor(MRef),
            safe_send(PoolName, {checkout_cancel, self()}),
            {error, pool_timeout}
    end.


%% @doc Return a  connection to specfied pool updating its timestamp
-spec checkin(Connection :: #conn{}) -> ok.
checkin(#conn{pool = PoolName} = Connection) ->
    safe_send(PoolName, {checkin, self(),
                         Connection#conn{updated = os:timestamp()}}),
    ok.


%% @doc Return a connection that has been closed.
-spec checkin_closed(Connection :: #conn{}) -> ok.
checkin_closed(#conn{pool = PoolName}) ->
    safe_send(PoolName, {checkin_closed, self()}),
    ok.


%% @doc Executes an operation

-spec transaction(Connection :: #conn{}, atom(), list()) ->
                         {NewConnection :: #conn{}, {ok, any()}} | {error, any()}.
transaction(#conn{worker_module = WorkerModule,
                  client = Client} = Conn, Function, Args) ->
    case WorkerModule:transaction(Client, Function, Args) of
        {error, Reason} -> {error, Reason};
        {NClient, Res} ->
            {Conn#conn{client = NClient}, Res}
    end.


close(#conn{worker_module = WorkerModule,
            client = Client}, Reason) ->
    WorkerModule:close(Client, Reason).


system_continue(Parent, Deb, State) ->
    pool_loop(State, Parent, Deb).


system_terminate(Reason, _Parent, _Deb, _State) ->
    exit(Reason).


%%%=============================================================================
%%% Internal functions
%%%=============================================================================

init(Parent, ClusterName, Host, Port, PoolName, WrkModule) ->
    case is_config_valid() of
        false ->
            proc_lib:init_ack(Parent, {error, invalid_config});
        true ->
            register(PoolName, self()),
            process_flag(trap_exit, true),
            Deb = sys:debug_options([]),
            {Module, Function} = mero_conf:stat_callback(),
            CallBackInfo = ?STATS_CONTEXT(Module, Function, ClusterName, Host, Port),
            N = mero_conf:initial_connections_per_pool(),
            mero_stat:log(CallBackInfo, "spawning initial ~p connections", [N]),
            [spawn_connect(PoolName, WrkModule, Host, Port, CallBackInfo) || _ <- lists:seq(1, N)],
            proc_lib:init_ack(Parent, {ok, self()}),
            State = #pool_st{
                       cluster = ClusterName,
                       free = [],
                       host = Host,
                       port = Port,
                       min_connections =
                           mero_conf:min_free_connections_per_pool(),
                       max_connections =
                           mero_conf:max_connections_per_pool(),
                       busy = dict:new(),
                       num_connected = 0,
                       num_connecting = N,
                       num_failed_connecting = 0,
                       reconnect_wait_time = ?RECONNECT_WAIT_TIME,
                       pool = PoolName,
                       stats_context = CallBackInfo,
                       worker_module = WrkModule},
            pool_loop(schedule_expiration(State), Parent, Deb)
    end.


%%% @doc Returns the specified PoolName state.
-spec state(PoolName :: atom()) -> term().
state(PoolName) ->
    MRef = erlang:monitor(process, PoolName),
    safe_send(PoolName, {state, {self(), MRef}}),
    receive
        {MRef, State} ->
            erlang:demonitor(MRef),
            PoolPid = whereis(PoolName),
            {links, Links} = process_info(PoolPid, links),
            {monitors, Monitors} = process_info(PoolPid, monitors),
            Free = length(State#pool_st.free),
            {message_queue_len, MessageQueueLength} = process_info(PoolPid, message_queue_len),

            [
             {message_queue_len, MessageQueueLength},
             {links, length(Links)},
             {monitors, length(Monitors)},
             {free, Free},
             {num_connected, State#pool_st.num_connected},
             {num_in_use, State#pool_st.num_connected - Free},
             {num_connecting, State#pool_st.num_connecting},
             {num_failed_connecting, State#pool_st.num_failed_connecting}
            ];
        {'DOWN', MRef, _, _, _} ->
            {error, down}
    after ?DEFAULT_TIMEOUT ->
            erlang:demonitor(MRef),
            {error, timeout}
    end.


%%%=============================================================================
%%% Internal functions
%%%=============================================================================

pool_loop(State, Parent, Deb) ->
    receive
        {connect_success, Conn} ->
            mero_stat:log("Yuhu new socket !", []),
            ?MODULE:pool_loop(connect_success(State, Conn), Parent, Deb);
        connect_failed ->
            mero_stat:log("Failed to create socket :(", []),
            ?MODULE:pool_loop(connect_failed(State), Parent, Deb);
        connect ->
            NumConnecting = State#pool_st.num_connecting,
            Connected = State#pool_st.num_connected,
            MaxConns = State#pool_st.max_connections,
            case (NumConnecting + Connected) > MaxConns of
                true ->
                    mero_stat:log(State#pool_st.stats_context, "Was connect time! but we ignored it ~p",
                                  [{NumConnecting, Connected, MaxConns}]),
                    ?MODULE:pool_loop(State#pool_st{num_connecting = NumConnecting - 1}, Parent, Deb);
                false ->
                    mero_stat:log(State#pool_st.stats_context, "Connect time! one ms", []),
                    spawn_connect(State#pool_st.pool,
                                  State#pool_st.worker_module,
                                  State#pool_st.host,
                                  State#pool_st.port,
                                  State#pool_st.stats_context),
                    ?MODULE:pool_loop(State, Parent, Deb)
            end;
        {checkout, From} ->
            ?MODULE:pool_loop(get_connection(State, From), Parent, Deb);
        {checkin, Pid, Conn} ->
            ?MODULE:pool_loop(checkin(State, Pid, Conn), Parent, Deb);
        {checkin_closed, Pid} ->
            ?MODULE:pool_loop(checkin_closed_pid(State, Pid), Parent, Deb);
        {checkout_cancel, Pid} ->
            ?MODULE:pool_loop(checkout_cancel(State, Pid), Parent, Deb);
        expire ->
            ?MODULE:pool_loop(schedule_expiration(expire_connections(State)), Parent, Deb);
        {state, {Pid, Ref}} ->
            safe_send(Pid, {Ref, State}),
            ?MODULE:pool_loop(State, Parent, Deb);
        {'DOWN', _, _, Pid, _} ->
            ?MODULE:pool_loop(down(State, Pid), Parent, Deb);
        {'EXIT', Parent, Reason} ->
            exit(Reason);
        %% Assume exit signal from connecting process
        {'EXIT', _, Reason} when Reason /= normal ->
            mero_stat:log("failed to connect and die !", []),
            ?MODULE:pool_loop(connect_failed(State), Parent, Deb);
        {system, From, Msg} ->
            sys:handle_system_msg(Msg, From, Parent, ?MODULE, Deb, State);
        _ ->
            ?MODULE:pool_loop(State, Parent, Deb)
    end.


get_connection(#pool_st{free = Free} = State, From) when Free /= [] ->
    give(State, From);
get_connection(State, {Pid, Ref} = _From) ->
    safe_send(Pid, {Ref, {reject, State}}),
    maybe_spawn_connect(State).


maybe_spawn_connect(#pool_st{
                       free = Free,
                       num_connected = Connected,
                       max_connections = MaxConn,
                       min_connections = MinConn,
                       num_connecting = Connecting,
                       num_failed_connecting = NumFailed,
                       worker_module = WrkModule,
                       stats_context = CallBackInfo,
                       reconnect_wait_time = WaitTime,
                       pool = Pool,
                       host = Host,
                       port = Port} = State) ->
    %% Length could be big.. better to not have more than a few dozens of sockets
    %% May be worth to keep track of the length of the free in a counter.

    FreeSockets = length(Free),

    Needed = calculate_needed(FreeSockets, Connected, Connecting, MaxConn, MinConn),
    if
        %% Need sockets and no failed connections are reported..
        %% we create new ones
        (Needed > 0), NumFailed < 1 ->
            mero_stat:log(CallBackInfo, "spawning needed ~p connections ~p", [Needed,
                                                                {FreeSockets, Connected, Connecting, MaxConn, MinConn}]),
            [spawn_connect(Pool, WrkModule, Host, Port, CallBackInfo)
             || _Number <- lists:seq(1, Needed)],
            State#pool_st{num_connecting = Connecting + Needed};

        %% Wait before reconnection if more than one successive
        %% connection attempt has failed. Don't open more than
        %% one connection until an attempt has succeeded again.
        (Needed > 0), Connecting == 0 ->
            mero_stat:log(CallBackInfo, "Wait before reconnection ~p connections ~p ms", [Needed, WaitTime]),
            erlang:send_after(WaitTime, self(), connect),
            State#pool_st{num_connecting = Connecting + 1};

        %% We dont need sockets or we have failed connections
        %% we wait before reconnecting.
        true ->
            State
    end.

calculate_needed(FreeSockets, Connected, Connecting, MaxConn, MinConn) ->
    TotalSockets = Connected + Connecting,
    MaxAllowed = MaxConn - TotalSockets,
    IdleSockets = FreeSockets + Connecting,
    case MinConn - IdleSockets of
        MaxNeeded when MaxNeeded > MaxAllowed ->
            MaxAllowed;
        MaxNeeded ->
            MaxNeeded
    end.


connect_success(#pool_st{free = Free,
                         num_connected = Num,
                         num_connecting = NumConnecting,
                         num_failed_connecting = NumFailed} = State,
                Conn) ->
    %% When we succeed the connection we assume
    NState = State#pool_st{free = [Conn | Free],
                           num_connected = Num + 1,
                           num_connecting = NumConnecting - 1,
                           num_failed_connecting = 0},
    case (NumFailed > 0) of
        true ->
            maybe_spawn_connect(NState);
        false ->
            NState
    end.


connect_failed(#pool_st{num_connecting = Num,
                        num_failed_connecting = NumFailed} = State) ->
    maybe_spawn_connect(State#pool_st{num_connecting = Num - 1,
                                      num_failed_connecting = NumFailed + 1}).


checkin(#pool_st{busy = Busy, free = Free} = State, Pid, Conn) ->
    case dict:find(Pid, Busy) of
        {ok, {MRef, _}} ->
            erlang:demonitor(MRef),
            State#pool_st{busy = dict:erase(Pid, Busy),
                          free = [Conn | Free]};
        error ->
            State
    end.


checkin_closed_pid(#pool_st{busy = Busy, num_connected = Num} = State, Pid) ->
    case dict:find(Pid, Busy) of
        {ok, {MRef, _}} ->
            erlang:demonitor(MRef),
            maybe_spawn_connect(State#pool_st{busy = dict:erase(Pid, Busy),
                                              num_connected = Num - 1
                                             });
        error ->
            State
    end.


down(#pool_st{busy = Busy, num_connected = Num} = State, Pid) ->
    case dict:find(Pid, Busy) of
        {ok, {_, Conn}} ->
            close_connection(Conn, down),
            NewState = State#pool_st{busy = dict:erase(Pid, Busy),
                                     num_connected = Num - 1},
            maybe_spawn_connect(NewState);
        error ->
            State
    end.


give(#pool_st{free = [Conn | Free],
              busy = Busy} = State, {Pid, Ref}) ->
    MRef = erlang:monitor(process, Pid),
    safe_send(Pid, {Ref, Conn}),
    State#pool_st{busy = dict:store(Pid, {MRef, Conn}, Busy), free = Free}.


spawn_connect(Pool, WrkModule, Host, Port, StatContext) ->
    spawn_link(
      fun() ->
              try
                  case connect(StatContext, WrkModule, Host, Port, StatContext) of
                      {ok, Client} ->
                          ?LOG_STAT_SPIRAL(StatContext, [spawn_connect_controlling_process]),
                          case controlling_process(StatContext, WrkModule, Client, whereis(Pool)) of
                              ok ->
                                  ?LOG_STAT_SPIRAL(StatContext, [connected]),
                                  safe_send(Pool, {connect_success, #conn{worker_module = WrkModule,
                                                                          client = Client,
                                                                          pool = Pool,
                                                                          updated = os:timestamp()
                                                                         }});
                              {error, _Reason} ->
                                  safe_send(Pool, connect_failed)
                          end;
                      {error, _Reason} ->
                          safe_send(Pool, connect_failed)
                  end
              catch E:R ->
                      ?LOG_STAT_SPIRAL(StatContext, [spawn_connect_error, {reason, exception}]),
                      mero_stat:log("Fatal problem connecting on mero ~p ~p ~p",
                                                [E, R, erlang:get_stacktrace()]),
                      safe_send(Pool, connect_failed)
              end
      end).


connect(StatContext, WrkModule, Host, Port, CallbackInfo) ->
    ?LOG_STAT_SPIRAL(StatContext, [spawn_connect_connect]),
    ?LOG_STAT_HISTOGRAM(StatContext, WrkModule, connect,
                        [Host, Port, CallbackInfo],
                        [time_connect]).

controlling_process(StatContext, WrkModule, WrkState, Parent) ->
    ?LOG_STAT_SPIRAL(StatContext, [spawn_connect_controlling_process]),
    ?LOG_STAT_HISTOGRAM(StatContext, WrkModule, controlling_process,
                        [WrkState, Parent],
                        [time_controlling_process]).


conn_time_to_live(_Pool) ->
    case mero_conf:connection_unused_max_time() of
        infinity -> infinity;
        Milliseconds -> Milliseconds * 1000
    end.


schedule_expiration(State) ->
    erlang:send_after(mero_conf:expiration_interval(), self(), expire),
    State.


expire_connections(#pool_st{free = Conns,
                            pool = Pool,
                            num_connected = Num} = State) ->
    Now = os:timestamp(),
    try conn_time_to_live(Pool) of
        TTL ->
            case lists:foldl(fun filter_expired/2, {Now, TTL, [], []}, Conns) of
                {_, _, [], _} -> State;
                {_, _, ExpConns, ActConns} ->
                    mero_stat:log("Expiring ~p connections", [length(ExpConns)]),
                    spawn_link(fun() -> close_connections(ExpConns, expired) end),
                    maybe_spawn_connect(
                      State#pool_st{free = ActConns,
                                    num_connected = Num - length(ExpConns)})
            end
    catch
        error:badarg -> ok
    end.


checkout_cancel(#pool_st{busy = Busy, free = Free} = State, Pid) ->
    case dict:find(Pid, Busy) of
        {ok, {MRef, Conn}} ->
            erlang:demonitor(MRef),
            State#pool_st{busy = dict:erase(Pid, Busy),
                          free = [Conn | Free]};
        error ->
            State
    end.


filter_expired(#conn{updated = Updated} = Conn, {Now, TTL, ExpConns, ActConns}) ->
    case timer:now_diff(Now, Updated) < TTL of
        true -> {Now, TTL, ExpConns, [Conn | ActConns]};
        false -> {Now, TTL, [Conn | ExpConns], ActConns}
    end.


safe_send(PoolName, Cmd) ->
    catch PoolName ! Cmd.

close_connections([], _Reason) -> ok;

close_connections([Conn | Conns], Reason) ->
    close_connection(Conn, Reason),
    close_connections(Conns, Reason).

close_connection(Conn, Reason) ->
    catch close(Conn, Reason).

is_config_valid() ->
    Initial = mero_conf:initial_connections_per_pool(),
    Max = mero_conf:max_connections_per_pool(),
    Min = mero_conf:min_free_connections_per_pool(),
    case (Min =< Initial) andalso (Initial =< Max) of
        true ->
            true;
        false ->
            error_logger:error_report(
              [{error, invalid_config},
               {min_connections, Min},
               {max_connections, Max},
               {initial_connections, Initial}]),
            false
    end.
