-module(replica_deliver).

-behaviour(gen_fsm).

%% API
-export([
  start_link/6,
  stop/1,
  new_data/1]).

%% gen_fsm callbacks
-export([init/1,
         disconnected/2,
         connected/2,
         handle_info/3,
         handle_event/3,
         terminate/3,
         code_change/4]).

-record(state, {
    timer,
    manager,
    recipient,
    server,
    protocol,
    max_points,
    socket,
    terminal,
    data_id}).

-define(TCP_OPTIONS, [binary, {active, true}, {reuseaddr, true}]).
-define(TIMEOUT, 30000).

-include_lib("logger/include/log.hrl").
%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Recipient, ServerID, ServerProto, ServerHost, ServerPort, MaxPoints) ->
  gen_fsm:start_link(?MODULE, {self(), Recipient, ServerID, ServerProto, ServerHost, ServerPort, MaxPoints}, []).

new_data(Pid) ->
  gen_fsm:send_all_state_event(Pid, new_data).

stop(Pid) ->
  gen_fsm:send_all_state_event(Pid, stop).
%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @spec init(Args) -> {ok, StateName, State} |
%%                     {ok, StateName, State, Timeout} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
%%--------------------------------------------------------------------
init({Manager, Recipient, ServerID, ServerProto, ServerHost, ServerPort, MaxPoints}) ->
  trace("init"),
  connect(),
  {ok, disconnected, #state{
      recipient = Recipient,
      server = {ServerID, ServerHost, ServerPort},
      protocol = ServerProto,
      max_points = MaxPoints,
      manager = Manager}, ?TIMEOUT}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same
%% name as the current state name StateName is called to handle
%% the event. It is also called if a timeout occurs.
%%
%% @spec state_name(Event, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
disconnected(connect, #state{
        manager = Manager,
        protocol = Proto,
        server = {ServerId, ServerHost, ServerPort}} = S) ->
  {ok, Socket} = gen_tcp:connect(ServerHost, ServerPort, ?TCP_OPTIONS),
  hooks:run(connection_accepted, [Proto, Socket]),
  hooks:final(connection_closed),
  {ok, Terminal} = replica_manager:get_terminal(Manager),
  auth(Proto:auth(Terminal), S#state{
        server = ServerId,
        terminal = Terminal,
        socket = Socket}).

connected(timeout, State) ->
  {stop, normal, State};
connected({send, [], <<>>}, #state{manager = Manager} = State) ->
  replica_manager:no_data(Manager),
  {next_state, connected, State, ?TIMEOUT};
connected({send, DataID, Data}, #state{socket = Socket} = S) ->
  debug("sending ~w", [Data]),
  gen_tcp:send(Socket, Data),
  Timer = gen_fsm:send_event_after(?TIMEOUT, timeout),
  {next_state, waiting_answer, S#state{data_id = DataID, timer = Timer}, ?TIMEOUT}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_event/[2,3], the instance of this function with
%% the same name as the current state name StateName is called to
%% handle the event.
%%
%% @spec state_name(Event, From, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_event(new_data, connected, #state{
        recipient = Recipient,
        protocol = Proto,
        server = ServerID,
        terminal = Terminal,
        max_points = Points} = State) ->
  [{Recipient, DataSet} | _] = hooks:run({Recipient, get}, [replica, data, {ServerID, Terminal, Points}]),
  debug("data set is ~w", [DataSet]),
  {DataIDs, Data} = filter_data(DataSet, Proto),
  debug("data ids ~w", [DataIDs]),
  Packet = Proto:prepare(Terminal, Data),
  connected({send, DataIDs, Packet}, State);
handle_event(new_data, StateName, State) ->
  {next_state, StateName, State, ?TIMEOUT};
handle_event(stop, _StateName, State) ->
  {stop, normal, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @spec handle_sync_event(Event, From, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @spec handle_info(Info,StateName,State)->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_info(info, StateName, State) ->
  alert("state name: ~w~n", [StateName]),
  alert("state: ~p~n", [State]),
  {next_state, StateName, State};
handle_info(exit, _StateName, State) ->
  {stop, normal, State};
handle_info({tcp_closed, Socket}, connected, #state{socket = Socket} = State) ->
  {stop, normal, State};
handle_info({tcp, Socket, Answer}, connected, #state{
        recipient = Recipient,
        socket = Socket} = State) ->
  hooks:run({Recipient, set}, [replica, add_answer, Answer]),
  {next_state, connected, State, ?TIMEOUT};
handle_info({tcp, Socket, Answer}, waiting_answer, #state{
        recipient = Recipient,
        data_id = DataIDs,
        manager = Manager,
        socket = Socket,
        protocol = gelix2nsk} = State) ->
  hooks:run({Recipient, set}, [replica, answer, {DataIDs, Answer}]),
  replica_manager:check_undelivered(Manager),
  {stop, normal, State};
handle_info({tcp, Socket, Answer}, waiting_answer, #state{
        timer = Timer,
        recipient = Recipient,
        socket = Socket,
        data_id = DataIDs} = S) ->
  hooks:run({Recipient, set}, [replica, answer, {DataIDs, Answer}]),
  new_data(self()),
  gen_fsm:cancel_timer(Timer),
  {next_state, connected, S#state{data_id = undefined, timer = undefined}, ?TIMEOUT}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, #state{socket = undefined}) ->
  ok;
terminate(_Reason, _StateName, #state{socket = Socket}) ->
  debug("terminating, socket is ~w", [Socket]),
  gen_tcp:close(Socket),
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
connect() ->
  gen_fsm:send_event(self(), connect).

filter_data(D, Proto) ->
  filter_data(D, Proto, [], []).

filter_data([], _Proto, IDs, DataList) ->
  {lists:reverse(IDs), lists:reverse(DataList)};
filter_data([H | T], Proto, IDs, DataList) ->
  {ID, Data} = case proplists:get_value(protocol, H) of
    Proto -> {proplists:get_value(id, H), proplists:get_value(data, H)};
    _Else -> {[], []}
  end,
  filter_data(T, Proto, [ID | IDs], [Data | DataList]).

auth(<<>>, State) ->
  new_data(self()),
  {next_state, connected, State, ?TIMEOUT};
auth(Data, #state{socket = Socket} = State) ->
  inet:setopts(Socket, [{active, false}]),
  debug("send auth data ~w", [Data]),
  ok = gen_tcp:send(Socket, Data),
  {ok, Answer} = gen_tcp:recv(Socket, 0, ?TIMEOUT),
  debug("auth recv: ~w", [Answer]),
  inet:setopts(Socket, [{active, true}]),
  new_data(self()),
  {next_state, connected, State, ?TIMEOUT}.
