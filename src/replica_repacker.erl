-module(replica_repacker).

-behaviour(gen_server).

%% API
-export([start_link/4]).
-export([repack/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {terminal_protocol, terminal_uin, server_info, server_protocol, filter = []}).

-include_lib("logger/include/log.hrl").
%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Terminal, ServerInfo, ServerProto, _Timeout) ->
  gen_server:start_link(?MODULE, {Terminal, ServerInfo, ServerProto}, []).

repack(_Pid, #{type := Type}, _Timeout)
    when ((Type =:= broken) or (Type =:= authorization) or (Type =:= unknown)) ->
  ok;
repack(Pid, Packet, Timeout) ->
  gen_server:call(Pid, {repack, Packet}, Timeout).

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
init({{TProto, UIN} = Terminal, ServerInfo, SProto}) ->
  trace("starting"),
  process_flag(trap_exit, true),
  hooks:run(get, [terminal, id, Terminal]),
  {ok, #state{
      terminal_protocol = TProto,
      terminal_uin = UIN,
      server_protocol = SProto,
      server_info = ServerInfo}}.

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
handle_call({repack, #{raw := RawData}},
            _From,
            #state{
               terminal_protocol = TProto,
               server_protocol = SProto,
               server_info = SInfo,
               filter = Filter} = State)
  when ((TProto =:= SProto) and
        (Filter =:= []))
       ->
  {reply, {ok, {SInfo, SProto, RawData}}, State};
handle_call({repack, Packet},
            _From,
            #state{
               server_protocol = SProto,
               server_info = SInfo,
               filter = Filter} = State) ->
  {ok, Reply} = SProto:pack(filter(Packet, Filter)),
  {reply, {ok, {SInfo, SProto, Reply}}, State};
handle_call(Request, From, State) ->
  warning("~w:unhandled call ~w", [From, Request]),
  {noreply, State}.

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
handle_cast(Msg, State) ->
  warning("unhandled cast ~w", [Msg]),
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
handle_info(Info, State) ->
  warning("unhandled info ~w", [Info]),
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
  trace("terminating"),
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
filter(Packet, []) ->
  Packet.
