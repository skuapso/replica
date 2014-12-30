-module(replica).

-behaviour(application).
-behaviour(supervisor).

%% hooks
-export([get_servers/4]).
-export([repack_packet/4]).
-export([new_data/6]).

%% API
-export([behaviour_info/1]).
-export([start/0]).
-export([start/2]).
-export([stop/1]).
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-include_lib("logger/include/log.hrl").

%%%===================================================================
%%% API functions
%%%===================================================================
behaviour_info(callbacks) ->
  [{auth, 1},
   {pack, 1},
   {max_points, 0},
   {prepare, 2}].

start() ->
  application:start(?MODULE).

start(_Type, Args) ->
  start_link(Args).

stop(_) ->
  ok.

get_servers(_Pid, Terminal, _Socket, Timeout) ->
  Servers = hooks:run(get, [?MODULE, servers, Terminal], Timeout),
  '_debug'("servers: ~w", [Servers]),
  ServersList = lists:map(
                  fun({Recipient, SList}) ->
                      lists:map(
                        fun([SId, SProto]) ->
                            {Recipient, SId, SProto}
                        end,
                        SList
                       )
                  end,
                  Servers
                 ),
  hooks:set({?MODULE, servers}, lists:flatten(ServersList)),
  ok.

repack_packet(_Pid, _Terminal, #{type := Type}, _Timeout)
    when ((Type =:= authentication) or (Type =:= broken) or (Type =:= unknown))
    ->
  ok;
repack_packet(_Pid, Terminal, Packet, Timeout) ->
  List = case hooks:get({replica, servers}) of
    undefined -> [];
    L -> L
  end,
  '_debug'("servers: ~w", [List]),
  lists:map(
    fun({Recipient, SId, SProto}) ->
        case catch repack(Terminal, Packet, SProto) of
          {'EXIT', Reason} ->
            '_info'("failed repack ~p", [{Terminal, SProto, Packet, Reason}]);
          {ok, BinData} ->
            hooks:run({Recipient, set}, [?MODULE, data, {SId, SProto, BinData}], Timeout),
            hooks:run({?MODULE, new_data}, [Recipient, SId, SProto, Terminal])
        end
    end,
    List
   ),
  ok.

new_data(Pid, Recipient, ServerID, ServerProto, Terminal, Timeout) when is_binary(ServerProto) ->
  new_data(Pid, Recipient, ServerID, binary_to_atom(ServerProto, latin1), Terminal, Timeout);
new_data(Pid, Recipient, ServerID, ServerProto, {Module, UIN}, Timeout) when is_binary(Module) ->
  new_data(Pid, Recipient, ServerID, ServerProto, {binary_to_atom(Module, latin1), UIN}, Timeout);
new_data(_Pid, Recipient, ServerID, ServerProto, {Module, _} = Terminal, _Timeout) ->
  '_trace'("new data for ~w:~w", [ServerID, Module]),
  ManagerPid = case supervisor:start_child(?MODULE,manager_spec(Recipient, ServerID, ServerProto)) of
    {ok, StartedPid} -> StartedPid;
    {error, {already_started, FoundPid}} -> FoundPid
  end,
  '_debug'("pid of manager is ~w", [ManagerPid]),
  replica_manager:new_data(ManagerPid, Terminal).

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {'_err'or, Error}
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
%%                     {'_err'or, Reason}
%% @end
%%--------------------------------------------------------------------
init(Args) ->
  '_trace'("starting"),
  Weight = misc:get_env(?MODULE, weight, Args),
  CronTimeout = misc:get_env(?MODULE, cron_timeout, Args) * 1000,
  hooks:install({terminal, connected}, Weight, {?MODULE, get_servers}),
  hooks:install({terminal, packet}, Weight, {?MODULE, repack_packet}),
  hooks:install({?MODULE, new_data}, Weight, {?MODULE, new_data}),
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

repack(Terminal, Packet, SProto) ->
  repack(Terminal, Packet, SProto, []).

repack({TProto, _TUin}, #{raw := Raw}, SProto, Filter)
  when (TProto =:= SProto)
       andalso (Filter =:= [])
       ->
  {ok, Raw};
repack({_TProto, _TUin} = _Terminal, Packet, SProto, Filter) ->
  SProto:pack(filter(Packet, Filter)).

filter(Packet, []) ->
  Packet.
