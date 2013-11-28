-module(replica).

-behaviour(application).
-behaviour(supervisor).

%% hooks
-export([
  get_servers/4,
  repack_packet/7,
  new_data/7
  ]).

%% API
-export([start/0, start/2, stop/1]).
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-include_lib("logger/include/log.hrl").
%%%===================================================================
%%% API functions
%%%===================================================================
start() ->
  application:start(?MODULE).

start(_Type, Args) ->
  start_link(Args).

stop(_) ->
  ok.

get_servers(_Pid, Module, UIN, Timeout) ->
  Servers = hooks:run(get, [?MODULE, servers, {Module, UIN}]),
  debug("servers: ~w", [Servers]),
  Pids = lists:flatten(lists:map(
        fun({ReplicaClient, ServerList}) ->
            lists:map(
              fun([ServerInfo, ServerProto]) ->
                  {ok, Pid} = replica_repacker:start_link(Module, UIN, ServerInfo, ServerProto, Timeout),
                  {ReplicaClient, Pid}
              end, ServerList)
        end, Servers)),
  hooks:set(replica_servers, Pids),
  ok.

repack_packet(_Pid, _Module, _UIN, Type, _RawData, _Packet, _Timeout)
    when ((Type =:= authentication) or (Type =:= broken) or (Type =:= unknown))
    ->
  ok;
repack_packet(_Pid, Module, UIN, Type, RawData, Packet, Timeout) ->
  Pids = case hooks:get(replica_servers) of
    undefined -> [];
    List -> List
  end,
  debug("servers: ~w", [Pids]),
  lists:map(
    fun({ReplicaClient, RPid}) ->
        {ok, {ServerID, ServerProto, _RawRepackedData} = Data} = replica_repacker:repack(RPid, Module, UIN, Type, RawData, Packet, Timeout),
        hooks:run({ReplicaClient, set}, [?MODULE, data, Data]),
        hooks:run({?MODULE, new_data}, [ReplicaClient, ServerID, ServerProto, Module, UIN])
    end, Pids),
  ok.

new_data(Pid, Recipient, ServerID, ServerProto, Module, UIN, Timeout) when is_binary(ServerProto) ->
  new_data(Pid, Recipient, ServerID, binary_to_atom(ServerProto, latin1), Module, UIN, Timeout);
new_data(Pid, Recipient, ServerID, ServerProto, Module, UIN, Timeout) when is_binary(Module) ->
  new_data(Pid, Recipient, ServerID, ServerProto, binary_to_atom(Module, latin1), UIN, Timeout);
new_data(_Pid, Recipient, ServerID, ServerProto, Module, UIN, _Timeout) ->
  trace("new data for ~w:~w", [ServerID, Module]),
  ManagerPid = case supervisor:start_child(?MODULE, manager_spec(Recipient, ServerID, ServerProto)) of
    {ok, StartedPid} -> StartedPid;
    {error, {already_started, FoundPid}} -> FoundPid
  end,
  debug("pid of manager is ~w", [ManagerPid]),
  replica_manager:new_data(ManagerPid, {Module, UIN}).

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Args) ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, Args).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init(Args) ->
  trace("starting"),
  Weight = misc:get_env(?MODULE, weight, Args),
  CronTimeout = misc:get_env(?MODULE, cron_timeout, Args) * 1000,
  hooks:install(terminal_uin, Weight, fun ?MODULE:get_servers/4),
  hooks:install(terminal_packet, Weight, fun ?MODULE:repack_packet/7),
  hooks:install({?MODULE, new_data}, Weight, fun ?MODULE:new_data/7),
  {
    ok,
    {
      {one_for_one, 5, 10},
      [
        {
          replica_cron,
          {replica_cron, start_link, [CronTimeout]},
          permanent,
          5000,
          worker,
          []
        }
      ]
    }
  }.

%%%===================================================================
%%% Internal functions
%%%===================================================================
manager_spec(Handler, ServerID, ServerProto) ->
  {
    {Handler, ServerID, ServerProto},
    {replica_manager, start_link, [Handler, ServerID, ServerProto]},
    transient,
    2000,
    worker,
    [replica_manager]
  }.
