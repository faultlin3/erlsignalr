-module(signalr).

-behaviour(gen_statem).

-callback handleInvocation(map()) -> any().

-export([start/4]).
-export([start/5]).
-export([stop/0]).

-export([invoke/2]).
-export([invokeBlocking/2]).
-export([cancel/1]).
-export([ping/0]).
-export([close/0]).

-export([terminate/3]).
-export([code_change/4]).
-export([init/1]).
-export([callback_mode/0]).

-export([wait_for_gun/3]).
-export([negotiate/3]).
-export([upgrade/3]).
-export([handshake/3]).
-export([ready/3]).


-define(RECORD_SEPARATOR, 30).

-record(data, {module = signalr, owner, connpid, endpoint, connectionId, path, buffer, response, negotiateVersion = 1, skipNegotiate = true, wsref, invocationCounter = 0, openInvocations = #{}}).

name() ->
    signalr_statem.

%% API

start(Module, Host, Port, Endpoint) ->
    start(Module, Host, Port, Endpoint, #{}).

start(Module, Host, Port, Endpoint, Opts) ->
    FullOpts0 = Opts#{module => Module, host => Host, port => Port, endpoint => Endpoint, owner => self()},
    FullOpts = maps:merge(#{cookie_store => gun_cookies_list:init()}, FullOpts0),
    gen_statem:start({local, name()}, ?MODULE, FullOpts, []).

stop() ->
    gen_statem:stop(name()).

invoke(Target, Arguments) ->
    gen_statem:call(name(), {invocation, Target, Arguments}).

invokeBlocking(Target, Arguments) ->
    gen_statem:call(name(), {invocationBlocking, Target, Arguments}).

cancel(InvocationId) ->
    gen_statem:call(name(), {cancel, InvocationId}).

ping() ->
    gen_statem:cast(name(), ping).

close() ->
    gen_statem:cast(name(), close).

%% General callbacks
terminate(_Reason, _State, _Data) ->
    void.

code_change(_Version, State, Data, _Extra) ->
    {ok, State, Data}.

init(#{module := Module, host := Host, port := Port, endpoint := Endpoint, owner := Owner, cookie_store := CookieStore} = _Opts) ->
    application:ensure_all_started(gun),
    HttpOpts = #{protocols => [http], cookie_store => CookieStore},
    {ok, ConnPid} = gun:open(Host, Port, HttpOpts),
    {ok, wait_for_gun, #data{module = Module, connpid=ConnPid, endpoint=Endpoint, owner=Owner}}.

callback_mode() ->
    [state_functions, state_enter].

%% State Callbacks

% pre_gun_up
wait_for_gun(enter, _, _) ->
    keep_state_and_data;

wait_for_gun(info, {gun_up, _ConnPid, _Protocol}, #data{skipNegotiate = SkipNegotiate, endpoint = Endpoint} = Data) ->
    case SkipNegotiate of
        true -> {next_state, upgrade,   Data#data{path=Endpoint}};
        _    -> {next_state, negotiate, Data#data{path=Endpoint}}
    end;

wait_for_gun({call, _}, _, _) ->
    {keep_state_and_data, postpone};

wait_for_gun(cast, _, _) ->
    {keep_state_and_data, postpone};

wait_for_gun(_EventType, _EventContent, _Data) ->
    stop.

% negotiate
negotiate(enter, _OldState, #data{connpid=ConnPid, endpoint=Endpoint, negotiateVersion=NegotiateVersion}) ->
    gun:post(ConnPid, filename:join(Endpoint, "negotiate?negotiateVersion=" ++ integer_to_list(NegotiateVersion)), [], ""),
    keep_state_and_data;

negotiate(info, {gun_response, _ConnPid, _StreamRef, nofin, _Status, _Headers}, #data{endpoint=Endpoint, negotiateVersion=NegotiateVersion} = Data) ->
    {ok, Body} = gun:await_body(_ConnPid, _StreamRef),
    Response = jiffy:decode(Body, [return_maps]),
    ConnectionId = case NegotiateVersion of
                       0 -> maps:get(<<"connectionId">>, Response);
                       _ -> maps:get(<<"connectionToken">>, Response)
                   end,
    Path = case is_binary(Endpoint) of
               true  -> <<Endpoint/binary, "?id=", ConnectionId/binary>>;
               false -> Endpoint ++ "?id=" ++ ConnectionId
           end,
    {next_state, upgrade, Data#data{path=Path, connectionId=ConnectionId, response=Response}};

negotiate({call, _}, _, _) ->
    {keep_state_and_data, postpone};

negotiate(cast, _, _) ->
    {keep_state_and_data, postpone};

negotiate(A, B, C) ->
    erlang:display({A, B, C}),
    stop.

% upgrade
upgrade(enter, _OldState, #data{path=Path, connpid=ConnPid} = Data) ->
    StreamRef = gun:ws_upgrade(ConnPid, Path),
    {keep_state, Data#data{wsref=StreamRef}};

upgrade(info, {gun_upgrade, _ConnPid, _StreamRef, _Protocols, _Headers}, Data) ->
    {next_state, handshake, Data};

upgrade({call, _}, _, _) ->
    {keep_state_and_data, postpone};

upgrade(cast, _, _) ->
    {keep_state_and_data, postpone};

upgrade(A, B, C) ->
    erlang:display({A, B, C}),
    stop.


% handshake
handshake(enter, _OldState, #data{connpid=ConnPid, wsref=StreamRef}) ->
    Msg = #{<<"protocol">> => <<"json">>, <<"version">> => 1},
    encodeAndSend(ConnPid, StreamRef, Msg),
    keep_state_and_data;

handshake(info, {gun_ws, _ConnPid, _StreamRef, {text, <<"{}", ?RECORD_SEPARATOR>>}}, Data) ->
    {next_state, ready, Data};

handshake({call, _}, _, _) ->
    {keep_state_and_data, postpone};

handshake(cast, _, _) ->
    {keep_state_and_data, postpone};

handshake(_, _, _) ->
    stop.

% ready
ready(enter, _OldState, #data{owner=Owner} = Data) ->
    Owner ! signalr_up,
    {keep_state, Data#data{buffer = <<>>}};

ready(info, {gun_ws, _ConnPid, _StreamRef, {text, Body}}, #data{buffer=Buffer} = Data) ->
    FullBody = <<Buffer/binary, Body/binary>>,
    case decode_prefix(FullBody, Data) of
        {clean} ->
            keep_state_and_data;
        {dirty, Trail} ->
            {keep_state, Data#data{buffer=Trail}}
    end;

ready({call, From}, {invocation, Target, Arguments}, #data{connpid=ConnPid, wsref=StreamRef, invocationCounter=Counter} = Data) ->
    InvocationId = genInvocationId(Counter),
    Msg = #{type => 1, target => Target, arguments => Arguments, invocationId => InvocationId},
    encodeAndSend(ConnPid, StreamRef, Msg),
    {keep_state, Data#data{invocationCounter = Counter + 1}, {reply, From, InvocationId}};

ready({call, From}, {invocationBlocking, Target, Arguments}, #data{connpid=ConnPid, wsref=StreamRef, invocationCounter=Counter, openInvocations=OpenInvocations} = Data) ->
    InvocationId = genInvocationId(Counter),
    Msg = #{type => 1, target => Target, arguments => Arguments, invocationId => InvocationId},
    encodeAndSend(ConnPid, StreamRef, Msg),
    {keep_state, Data#data{invocationCounter = Counter + 1, openInvocations = OpenInvocations#{InvocationId => From}}};

ready(cast, {invocationReply, InvocationId, Reply}, #data{openInvocations=OpenInvocations} = Data) ->
    case maps:take(InvocationId, OpenInvocations) of
        {From, NewOpenInvocations}->
            NewData = Data#data{openInvocations = NewOpenInvocations},
            case maps:is_key(<<"error">>, Reply) of
                true ->
                    {keep_state, NewData, {reply, From, {signalr_fail, Reply}}};
                false ->
                    {keep_state, NewData, {reply, From, {signalr_success, Reply}}}
            end;
        error ->
            keep_state_and_data
    end;

ready({call, From}, {streamInvocation, Target, Arguments}, #data{connpid=ConnPid, wsref=StreamRef, invocationCounter=Counter} = Data) ->
    InvocationId = genInvocationId(Counter),
    Msg = #{type => 4, target => Target, arguments => Arguments, invocationId => InvocationId},
    encodeAndSend(ConnPid, StreamRef, Msg),
    {keep_state, Data#data{invocationCounter = Counter + 1}, {reply, From, InvocationId}};

ready({call, From}, {streamItem, InvocationId, Item}, #data{connpid=ConnPid, wsref=StreamRef}) ->
    Msg = #{type => 2, invocationId => list_to_binary(integer_to_list(InvocationId)), item => Item},
    encodeAndSend(ConnPid, StreamRef, Msg),
    {keep_state_and_data, {reply, From, InvocationId}};

ready({call, From}, {completion, InvocationId}, #data{connpid=ConnPid, wsref=StreamRef}) ->
    Msg = #{type => 3, invocationId => list_to_binary(integer_to_list(InvocationId))},
    encodeAndSend(ConnPid, StreamRef, Msg),
    {keep_state_and_data, {reply, From, InvocationId}};

ready({call, From}, {cancel, InvocationId}, #data{connpid=ConnPid, wsref=StreamRef}) ->
    Msg = #{type => 5, invocationId => list_to_binary(integer_to_list(InvocationId))},
    encodeAndSend(ConnPid, StreamRef, Msg),
    {keep_state_and_data, {reply, From, InvocationId}};

ready(cast, ping, #data{connpid=ConnPid, wsref=StreamRef}) ->
    Msg = #{type => 6},
    encodeAndSend(ConnPid, StreamRef, Msg),
    keep_state_and_data;

ready(cast, close, #data{connpid=ConnPid, wsref=StreamRef}) ->
    Msg = #{type => 7},
    encodeAndSend(ConnPid, StreamRef, Msg),
    keep_state_and_data;

ready({call, From}, {close, Error}, #data{connpid=ConnPid, wsref=StreamRef}) ->
    Msg = #{type => 7, error => Error},
    encodeAndSend(ConnPid, StreamRef, Msg),
    {keep_state_and_data, [{reply, From, ok}]};

ready(_, _, #data{owner=Owner}) ->
    Owner ! signalr_down,
    stop.

% Utility functions
encodeAndSend(ConnPid, StreamRef, Map) ->
    Enc = jiffy:encode(Map),
    gun:ws_send(ConnPid, StreamRef, {text, <<Enc/binary, ?RECORD_SEPARATOR>>}).

handle(Msg, #data{module=Module, owner=Owner}) ->
    case maps:get(<<"type">>, Msg) of
        1 -> Module:handleInvocation(Msg);
        3 -> gen_statem:cast(name(), {invocationReply, maps:get(<<"invocationId">>, Msg), Msg});
        6 -> gen_statem:cast(name(), ping);
        7 -> Owner ! signalr_close;
        _ -> void
    end.

decode_prefix(Binary, Data) ->
    try jiffy:decode(Binary, [return_maps, return_trailer]) of
        {has_trailer, Msg, <<30, Trailer/binary>>} ->
            handle(Msg, Data),
            decode_prefix(Trailer, Data);
        Msg ->
            handle(Msg, Data),
            {clean}
    catch
        error:{1, truncated_json} ->
            {dirty, Binary}
    end.

genInvocationId(InvocationCounter) ->
    list_to_binary(integer_to_list(InvocationCounter)).
