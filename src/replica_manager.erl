-module(replica_manager).

-behaviour(gen_server).

%% API
-export([
  start_link/3,
  new_data/2,
  get_terminal/1,
  no_data/1,
  check_undelivered/1
  ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
    recipient,
    server_id,
    server_protocol,
    server_host,
    server_port,
    max_points,
    max_connections,
    type,
    connected = 0,
    waiting = 0,
    ets,
    retry_interval,
    status = connected
    }).

-include_lib("logger/include/log.hrl").
%%%===================================================================
%%% API
%%%===================================================================
new_data(Pid, Terminal) ->
  gen_server:cast(Pid, {new_data, Terminal}).

get_terminal(Pid) ->
  gen_server:call(Pid, get_terminal).

no_data(Pid) ->
  gen_server:cast(Pid, {no_data, self()}).

check_undelivered(Pid) ->
  gen_server:cast(Pid, check_undelivered).
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Handler, ServerID, ServerProto) ->
  gen_server:start_link(?MODULE, {Handler, ServerID, ServerProto}, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init({Recipient, ServerID, ServerProto}) ->
  trace("init: recipient ~w, server id ~w, server protocol ~w", [Recipient, ServerID, ServerProto]),
  process_flag(trap_exit, true),
  [{Recipient, [Params]} | _] = hooks:run({Recipient, get}, [replica, servers, [{id, ServerID}]]),
  debug("server options ~w", [Params]),
  ServerHost = binary_to_list(proplists:get_value(hostname, Params)),
  ServerPort = proplists:get_value(port, Params),
  MaxPoints = min(proplists:get_value(max_points, Params, 1), ServerProto:max_points()),
  RetryInterval = round(misc:interval2seconds(proplists:get_value(retry_interval, Params)) * 1000),
  MaxConnections = proplists:get_value(max_connections, Params, infinity),
  Type = proplists:get_value(connection_type, Params, soft),
  debug("server: ~w:~w, max point: ~w, max connections ~w, type ~w, retry interval ~w",
        [ServerHost, ServerPort, MaxPoints, MaxConnections, Type, RetryInterval]),
  ETS = ets:new(?MODULE, [set, public]),
  timer:send_interval(RetryInterval, force_connect),
  {ok, #state{
      recipient = Recipient,
      ets = ETS,
      max_connections = MaxConnections,
      retry_interval = RetryInterval,
      server_id = ServerID,
      server_protocol = ServerProto,
      server_host = ServerHost,
      server_port = ServerPort,
      type = Type,
      max_points = MaxPoints}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(get_terminal, {From, _}, #state{
        connected = Connected,
        waiting = Waiting,
        ets = ETS} = State) when Waiting > 0 ->
  trace("getting terminal for ~w", [From]),
  {[[Terminal]], _} = ets:match(ETS, {'$1', waiting}, 1),
  ets:delete(ETS, Terminal),
  ets:insert(ETS, {From, {Terminal, active}}),
  connect(),
  debug("connected/waiting: ~w/~w", [Connected + 1, Waiting - 1]),
  {reply, {ok, Terminal}, State#state{
      status = connected,
      connected = Connected + 1,
      waiting = Waiting - 1}};
handle_call(get_terminal, {From, _}, #state{
        waiting = 0,
        ets = ETS} = State) ->
  err("getting terminal for ~w when waiting=0, ets is ~w", [From, ets:match(ETS, '$1')]),
  {reply, ok, State};
handle_call(Request, From, State) ->
  crit("unhandled request ~w from ~w when ~w", [Request, From, State]),
  {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({new_data, Terminal}, #state{ets = ETS} = State) ->
  trace("new data for terminal ~w", [Terminal]),
  NewState = case deliver(ETS, Terminal) of
    undefined ->
      trace("adding terminal ~w", [Terminal]),
      add_terminal(Terminal, State);
    {ok, Pid} ->
      debug("notice to deliver ~w about new data", [Pid]),
      replica_deliver:new_data(Pid),
      State
  end,
  {noreply, NewState};

handle_cast({no_data, Pid}, #state{
        waiting = Waiting,
        connected = Connected,
        max_connections = MaxConnections} = State)
    when (Waiting + Connected > MaxConnections) ->
  trace("no data for ~w", [Pid]),
  debug("stopping ~w", [Pid]),
  replica_deliver:stop(Pid),
  {noreply, State};
handle_cast({no_data, Pid}, #state{
        ets = ETS} = State) ->
  trace("no data for ~w", [Pid]),
  [[Terminal]] = ets:match(ETS, {Pid, {'$1', '_'}}),
  ets:insert(ETS, {Pid, {Terminal, no_data}}),
  {noreply, State};

handle_cast(connect, #state{waiting = Waiting, status = connected} = State)
    when (Waiting > 0) ->
  handle_cast(force_connect, State);
handle_cast(connect, State) ->
  {noreply, State};
handle_cast(force_connect, #state{status = connecting, type = soft} = State) ->
  {noreply, State};
handle_cast(force_connect, #state{
        waiting = Waiting,
        recipient = Recipient,
        server_id = ServerID,
        server_protocol = ServerProto,
        server_host = ServerHost,
        server_port = ServerPort,
        max_points = MaxPoints,
        ets = ETS} = State)
    when (Waiting > 0) ->
  Connecting = ets:select_count(ETS, [{{'$1',connecting},[],[true]}]),
  debug("connecting/waiting ~w/~w", [Connecting, Waiting]),
  if
    Waiting > Connecting ->
      {ok, Pid} = replica_deliver:start_link(
          Recipient,
          ServerID,
          ServerProto,
          ServerHost,
          ServerPort,
          MaxPoints),
      debug("new deliver ~w", [Pid]),
      ets:insert(ETS, {Pid, connecting});
    true -> debug("waiting <= connecting, no new connection")
  end,
  {noreply, State#state{status = connecting}};
handle_cast(force_connect, State) ->
  {noreply, State};

handle_cast(check_undelivered, #state{
        connected = 0,
        waiting = Waiting,
        max_connections = MaxConnections,
        recipient = Recipient,
        server_id = ServerID} = State)
    when (Waiting < MaxConnections) ->
  F = fun() ->
      lists:map(fun({R, Data}) ->
            R = Recipient,
            lists:map(fun(X) ->
                  ServerID = proplists:get_value(server_id, X),
                  Proto = proplists:get_value(protocol, X),
                  TerminalUIN = proplists:get_value(terminal_uin, X),
                  TerminalProto = proplists:get_value(terminal_protocol, X),
                  hooks:run({replica, new_data}, [Recipient,
                                                  ServerID,
                                                  Proto,
                                                  TerminalProto,
                                                  TerminalUIN])
              end, Data)
        end, hooks:run({Recipient, get}, [replica, undelivered, ServerID]))
  end,
  spawn(F),
  {noreply, State};
handle_cast(check_undelivered, State) ->
  {noreply, State};

handle_cast(Msg, State) ->
  crit("unhandled cast msg ~w when ~w", [Msg, State]),
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({'EXIT', Pid, normal}, #state{
        status = Status,
        ets = ETS,
        connected = Connected} = State) ->
  trace("deliver exited"),
  {Add, NewStatus} = case ets:match(ETS, {Pid, '$1'}, 1) of
    {[[connecting]], _}  ->
      if
        Status =:= connecting -> {0, issue};
        true -> {0, Status}
      end;
    '$end_of_table'     ->
      err("unregistered pid ~w", [Pid]),
      debug("ets: ~w", [ets:match(ETS, '$1')]),
      {0, Status};
    _Else               -> {1, Status}
  end,
  ets:delete(ETS, Pid),
  check_undelivered(self()),
  {noreply, State#state{connected = Connected - Add, status = NewStatus}};
handle_info({'EXIT', Pid, Reason}, State) ->
  Status = handle_issue(Reason, State),
  handle_info({'EXIT', Pid, normal}, State#state{status = Status});

handle_info(force_connect, State) ->
  handle_cast(force_connect, State);
handle_info(Info, State) ->
  crit("unhandled info msg ~w when ~w", [Info, State]),
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
deliver(ETS, Terminal) ->
  trace("getting deliver for terminal ~w", [Terminal]),
  case ets:match(ETS, {'$1', {Terminal, '_'}}) of
    [] -> undefined;
    [[Pid]] -> {ok, Pid}
  end.

connect() ->
  gen_server:cast(self(), connect).

add_terminal(Terminal, #state{
        ets = ETS,
        max_connections = MaxConnections,
        waiting = Waiting,
        connected = Connected} = State)
    when ((Waiting + Connected) >= MaxConnections) ->
  Add = case ets:match(ETS, {'$1', {'_', no_data}}, 1) of
    '$end_of_table' -> 0;
    {[[Pid]], _} ->
      replica_deliver:stop(Pid),
      connect(),
      add_terminal1(Terminal, State)
  end,
  State#state{waiting = Waiting + Add};
add_terminal(Terminal, #state{waiting = Waiting} = State) ->
  Add = add_terminal1(Terminal, State),
  State#state{waiting = Waiting + Add}.

add_terminal1(Terminal, #state{ets = ETS}) ->
  case ets:match(ETS, {Terminal, '_'}, 1) of
    '$end_of_table' ->
      connect(),
      debug("adding terminal ~w", [Terminal]),
      ets:insert(ETS, {Terminal, waiting}),
      1;
    _ -> 0
  end.

handle_issue(_Reason, #state{status = issue}) ->
  issue;
handle_issue(Reason, #state{
                        server_id = ServerId,
                        server_host = ServerHost,
                        server_port = ServerPort,
                        server_protocol = ServerProto,
                        connected = 0}) ->
  warning("issue while delivering to ~w: ~w", [{ServerId, ServerHost, ServerPort, ServerProto},
                                               Reason]),
  issue;
handle_issue(Reason, #state{
				server_id = ServerId,
				server_host = ServerHost,
				server_port = ServerPort,
				server_protocol = ServerProto,
				status = Status}) ->
  info("server ~w issue ~w", [{ServerId, ServerHost, ServerPort, ServerProto},Reason]),
  Status.
