%%%-------------------------------------------------------------------
%%% File    : ejabberd_ndb.erl
%%% Author  : Muhammad Naeem <m.naemakram@gmail.com>
%%% Purpose : Interface for ndb database
%%% Created : 13 Oct 2017 by by Muhmammad Naeem <m.naemakram@gmail.com>
%%%
%%%
%%% lynk-ejabberd, Copyright (C) 2017   Whizpool
%%%
%%% This program is free software; you can redistribute it and/or
%%% modify it under the terms of the GNU General Public License as
%%% published by the Free Software Foundation; either version 2 of the
%%% License, or (at your option) any later version.
%%%
%%% This program is distributed in the hope that it will be useful,
%%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
%%% General Public License for more details.
%%%
%%% You should have received a copy of the GNU General Public License along
%%% with this program; if not, write to the Free Software Foundation, Inc.,
%%% 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
%%%
%%%-------------------------------------------------------------------

-module(ejabberd_ndb).

-behaviour(gen_server).

%% API
-export([start_link/5, get_proc/1,  is_connected/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
		terminate/2, code_change/3]).

		
%% Function defination related Auth Module
-export([get_all_users/1, get_users_count/1, get_user_password/2, set_user_password/2, add_user_password/2, del_user/2]).


%% Function defination related Offline Module
-export([store_offline_message/7, get_offline_messages/2]).


%% Function defination related MUC Module
-export([store_room/7, restore_room/2, forget_room/2, get_rooms/1, get_nick/2, make_muc_opt/6]).


%% Function defination related MAM Module
-export([store_archive/5, delete_archive/2, delete_old_messages/3]).


%% Function defination related VCARD Module
-export([get_vcard/2 ,set_vcard/12 ]).



%% Function defination related PRIVACY Module
-export([get_user_list/2, get_privacy/2, set_privacy/4]).


%% Function defination related LAST SEEN Module
-export([store_last_info/4, get_last/2]).

%%Sending push 
-export([send_push_message/7,send_voice_push_message/8]).

%%Store Media Data
-export([store_media_data/1]).
	
%%Conversion Functions
-export([subcribers_to_scylla/1,affiliations_to_scylla/1,scylla_to_affiliations/1,scylla_to_subscribers/1]).

-include("jid.hrl").	
-include("ejabberd.hrl").
-include("logger.hrl").
-include("fxml.hrl").
-include("erlcass_ndb.hrl").
-include("muc_sub.hrl").



-record(state, {pid = self() :: pid()}).

%%%===================================================================
%%% erlCass Include Files 
%%%===================================================================		
-include("erlcass.hrl").

%%%===================================================================
%%% API
%%%===================================================================
%% @private
start_link(Num, Server, Port, _StartInterval, Options) ->
    gen_server:start_link({local, get_proc(Num)}, ?MODULE, [Server, Port, Options], []).

%% @private
is_connected() ->
    lists:all(
      fun({_Id, Pid, _Type, _Modules}) when is_pid(Pid) ->
	      case catch ndbc_pb_socket:is_connected(get_ndb_pid(Pid)) of
		  true -> true;
		  _ -> false
	      end;
	 (_) ->
	      false
      end, supervisor:which_children(ejabberd_ndb_sup)).

%% @private
get_proc(I) ->
    misc:binary_to_atom(
      iolist_to_binary(
	[atom_to_list(?MODULE), $_, integer_to_list(I)])).

%%%===================================================================
%%% Auth related functions
%%%===================================================================

%% Authentication Data
get_all_users(_Host) ->
	%%Not Implemented yet
[].

%%TOTAL USER COUNT 
get_users_count(_Host) ->
	%%Not Implemented yet
0.


%%GET PASSWORD
get_user_password(User, _Host) ->
	SelectCols = [{<<"privatekey">>, text}],
	BigIntUser = list_to_integer(binary_to_list(User)),
	{ok, SelectCols, Result} = erlcass:execute(?AUTH, ?BIND_BY_NAME, [{<<"userid">>, BigIntUser}]),
	if (length(Result) > 0 ) ->	
		[[PrivateKey]] = Result,
		{ok ,PrivateKey};
	true->
		ok
	end.

%%Updating user Password
set_user_password(User, Passwd) ->
	BigIntUser = list_to_integer(binary_to_list(User)),
	ok = erlcass:execute(?UPDATE_PASSWORD, [Passwd, BigIntUser]),
ok.

%%Add user Auth
add_user_password(User, Passwd) ->
	BigIntUser = list_to_integer(binary_to_list(User)),
	ok = erlcass:execute(?ADD_AUTH, [<<"Active">>,  BigIntUser, Passwd]),
ok.


del_user(User, _Server) ->
	BigIntUser = list_to_integer(binary_to_list(User)),
   	ok = erlcass:execute(?DEL_AUTH, ?BIND_BY_NAME, [{<<"userid">>, BigIntUser}]),	
ok.

%%%===================================================================
%%% MUC related functions
%%%===================================================================

%% Save room based on host	
store_room(Name, _Host, Opts, Title, Desc, Aff, Subs) ->
	CreateDate = p1_time_compat:system_time(micro_seconds),
	erlcass:execute(?ADD_ROOM, [Name, Opts, CreateDate, Title, Desc, Aff, Subs]),
	
{ok,Name}.


%%get user nick , currently not in use 	
get_nick(_Nick, _Host) ->
	%% Need to update,
	%% If required then we will update it.
{ok, []}.

%%%Restore Room 
restore_room(Name, _Host) ->
	SelectCols = [{<<"groupid">>,text},{<<"opts">>,text},{<<"title">>,text},{<<"description">>,text},{<<"affiliations">>,{map, bigint, text}},{<<"subscribers">>,{map, bigint, text}}],				  
	{ok , SelectCols ,Result } =  erlcass:execute(?RESTORE_ROOM, ?BIND_BY_NAME, [{<<"groupid">>, Name}]),	
	Opts = if (length(Result) > 0 ) ->
			[[GroupID, SOpts, Title, Description, Affiliations, Subscribers]] = Result,
			make_muc_opt(SOpts, Title, Description, Affiliations, Subscribers, GroupID);
		true->
			<<"">>
	end,
	
{ok,Opts}.

%% Destroy room based on host
forget_room(Name, _Host) ->
	ok = erlcass:execute(?FORGET_ROOM, ?BIND_BY_NAME, [{<<"groupid">>, Name}]),	
ok.

%% Get list of room based on host
get_rooms(_Host) ->
	SelectCols = [{<<"groupid">>,text},{<<"opts">>,text}, {<<"title">>,text}, {<<"description">>,text}, {<<"affiliations">>,{map, bigint, text}}, {<<"subscribers">>,{map, bigint, text}}],			  
	{ok , SelectCols ,Result } =  erlcass:query(<<"SELECT groupid, opts,title,description,affiliations,subscribers FROM chats.groups">>),

{ok, Result}.


%%===================================================================
%%% MAM related functions
%%%===================================================================

%% Saving Archive Data
store_archive(Type ,SenderID, ReceiverID, MsgBody, MetaData) ->
	TSAddinteger = p1_time_compat:system_time(micro_seconds),		
	erlcass:execute(?ARCHIVE_MSG, [Type, SenderID, ReceiverID, MsgBody, MetaData, TSAddinteger]),
{ok,<<"0">>}.

%% Delele Archive Message , currently not in use
delete_archive(_LUser, _LServer) ->
	%%Not Implemented yet
ok.

%% Delete old message , currently not in use
delete_old_messages(_ServerHost,_TS,_TypeClause) ->
	%%Not Implemented yet
ok.


%%%===================================================================
%%% VCARD related functions
%%%===================================================================

%% get VCARD / profile data
get_vcard(LUser, _LServer)->
	%%% Here is the query to fetch the records. 
	%%% We are fetching all profiles
	SelectCols = [{<<"userid">>,bigint},{<<"type">>,int},{<<"name">>,text},{<<"image">>,text},{<<"status">>,text},{<<"stickers">>,int},{<<"seen_receipt">>,int},{<<"sharedwallpapers">>,int},{<<"imageupdate">>,text},{<<"publicinfo">>,int},{<<"modifydate">>,bigint},{<<"storage">>,text},{<<"version">>,int}],
	
	%%{BigIntUser,[]} = string:to_integer(binary_to_list(LUser)),
	BigIntUser = list_to_integer(binary_to_list(LUser)),
	{ok , SelectCols ,Result } =  erlcass:execute(?GET_VCARD, ?BIND_BY_NAME, [{<<"userid">>, BigIntUser}]),

				
	if (length(Result) > 0 ) ->	
	
		%%%===================================================================
		%% Here we are getting all type of profile and sending in one stanza 
		%% rather  than client sent multiple request for the different profile 
		%% We have modified the code in such a way that one query will return all the profiles for one user. 
		%%%===================================================================

		 VcardData = {xmlel,<<"vCard">>,	
				[{<<"xmlns">>,<<"vcard-temp">>}],lists:map(fun(Rs) ->
				[_UID,Type,Name,Image,Status,Stickers,LastSeen,Wallpapers,ImageUpdate,Seenby,Modifydate,Storage,Version] = Rs,
				ProfileType = if Type == 1 ->
							<<"public">>;
						true->
							if Type == 2 ->
								<<"private">>;
							true->
								<<"other">>
							end
						end,
				{xmlel,ProfileType,[],[{xmlel,<<"name">>,[],[{xmlcdata,Name}]},{xmlel,<<"image">>,[],[{xmlcdata,Image}]},{xmlel,<<"status">>,[],[{xmlcdata,Status}]},{xmlel,<<"public_info">>,[],[{xmlcdata,integer_to_list(Seenby)}]},{xmlel,<<"stickers">>,[],[{xmlcdata,integer_to_list(Stickers)}]},{xmlel,<<"seen_receipt">>,[],[{xmlcdata,integer_to_list(LastSeen)}]},{xmlel,<<"acceptwallpapers">>,[],[{xmlcdata,integer_to_list(Wallpapers)}]},{xmlel,<<"imageupdate">>,[],[{xmlcdata,ImageUpdate}]},{xmlel,<<"modifydate">>,[],[{xmlcdata,integer_to_list(Modifydate)}]},{xmlel,<<"storage">>,[],[{xmlcdata,Storage}]},{xmlel,<<"version">>,[],[{xmlcdata,integer_to_list(Version)}]}]}
				
			end, Result)},		
		{ok, VcardData};
	true->
		[]
	end.


	
	
%% set VCARD / profile data
set_vcard(LUser,LServer, ProfileType, Name, Image, Status,Stickers,SeenReceipt,SharedWallpapers,ImageUpdate,PublicInfoSeenBy,Storage)->
	%%{BigIntUser,[]} = string:to_integer(binary_to_list(LUser)),
	%%{Type,[]} = string:to_integer(binary_to_list(ProfileType)),
	%%{StickersAllowed,[]} = string:to_integer(binary_to_list(Stickers)),	
	%%{SeenReceiptAllowed,[]} = string:to_integer(binary_to_list(SeenReceipt)),	
	%%{SharedWallpapersAllowed,[]} = string:to_integer(binary_to_list(SharedWallpapers)),	
	%%{PublicInfo,[]} = string:to_integer(binary_to_list(PublicInfoSeenBy)),
	BigIntUser = list_to_integer(binary_to_list(LUser)),
	Type = list_to_integer(binary_to_list(ProfileType)),
	StickersAllowed = list_to_integer(binary_to_list(Stickers)),
	SeenReceiptAllowed = list_to_integer(binary_to_list(SeenReceipt)),
	SharedWallpapersAllowed = list_to_integer(binary_to_list(SharedWallpapers)),
	PublicInfo = list_to_integer(binary_to_list(PublicInfoSeenBy)),
	Version = 1,
	%%%===================================================================
	%% As this function is used to add and update the configuration 
	%% So first we need to check whether this user profile exist or not 
	%% If its already exit then we just need to update it. Don't need to insert New record.	
	%% By Default Scylla will not add any new record if both primary and cluster key is same 
	%% So we don't need to check records exist just simple Added the insert statement will work.
	%%%===================================================================
	
	TSAddinteger = p1_time_compat:system_time(micro_seconds),	
	erlcass:execute(?SET_VCARD, [BigIntUser,Type, Name, Image, Status, StickersAllowed,SeenReceiptAllowed,SharedWallpapersAllowed,ImageUpdate,PublicInfo, TSAddinteger,Storage,Version]),
	
	%%%===================================================================
	%% SEND Push notification to user contacts when ever user update anything.
	%%%===================================================================
	SelectCols = [{<<"userid">>,bigint}],	
	%%{BigIntUser,[]} = string:to_integer(binary_to_list(LUser)),
	BigIntUser = list_to_integer(binary_to_list(LUser)),
	{ok , SelectCols ,Result } =  erlcass:execute(?GET_USERCONTACTS, ?BIND_BY_NAME, [{<<"contactid">>, BigIntUser}]),

	if (length(Result) > 0 ) ->
		
		SubscribersList = lists:map(fun (R) ->
				[Subscribers] = R,
				integer_to_list(Subscribers)
		end, Result),
		Subscribers = list_to_binary(string:join(SubscribersList,",")),		
		send_contact_push(LUser, LServer,Subscribers, Type, Name, Image, Status, StickersAllowed, SeenReceiptAllowed, SharedWallpapersAllowed, ImageUpdate,Storage,Version);
	true->
		ok
	end,		
	
	
ok.



%%%===================================================================
%%% Mod Offline related functions
%%%===================================================================


%%Saving offline Data
store_offline_message(From, To, MsgText , MetaData, Type, MsgID,TS)->
	ok = erlcass:execute(?STORE_OFFLINE, [To, From, TS, MsgText, MetaData, Type, MsgID]),
ok.


%%get the Offline Message based on user and host
get_offline_messages(User, _Host) ->
	
	%%% Here is the query to fetch the records.
	SelectCols = [{<<"senderid">>,text},
              {<<"messagetext">>,text},
              {<<"metadata">>,text},
              {<<"type">>,text},
              {<<"createddate">>,bigint},
              {<<"msgid">>,text}],
	
	%% As we need string so we don't need to conversion here		  
	
	{ok , SelectCols ,Result } =  erlcass:execute(?GET_OFFLINE, ?BIND_BY_NAME, [{<<"userid">>, User}]),
	
		%%% After fetching all the records now we need to remove these records from the database 
		%%% so next we don't want to fetch these records.	
		%% Here we need to check either we have any message or not 
		%% If don't have message than we don't need to run the query for delete the records
	if (length(Result) > 0 ) ->
		ok = erlcass:execute(?DEL_OFFLINE, ?BIND_BY_NAME, [{<<"userid">>, User}]),	
		Result;
	true->
		[]
	end.	


%%%===================================================================
%%% Privacy related functions
%%%===================================================================

%% Blocking & Privacy Functions 
get_user_list(_LUser, _LServer)->
	%%Not Implemented yet
{none, []}.

%% GET user privacy list
get_privacy(LUser, _LServer)->
	SelectCols = [{<<"userid">>,bigint},{<<"listdata">>,text}],	
	%%{BigIntUser,[]} = string:to_integer(binary_to_list(LUser)),	
	BigIntUser = list_to_integer(binary_to_list(LUser)),
	{ok , SelectCols ,Result } =  erlcass:execute(?GET_PRIVACY, ?BIND_BY_NAME, [{<<"userid">>, BigIntUser}]),	
	if (length(Result) > 0 ) ->
			[[_UserID, UserList]] = Result,
			{_, MyList} = jlib:base64_to_term(UserList),
			{ok, MyList};	
		true->
			{error, notfound}
	end.

%%Update Privacy list for singel user
set_privacy(LUser, _LServer,Default, NewLists)->

	%%{BigIntUser,[]} = string:to_integer(binary_to_list(LUser)),	
	BigIntUser = list_to_integer(binary_to_list(LUser)),	
	PLists = jlib:term_to_base64(NewLists), 
	%%PLists = NewLists, 
	%%%===================================================================
	%% As this function is used to add and update the configuration 
	%% So first we need to check whether this user profile exist or not 
	%% If its already exit then we just need to update it. Don't need to insert New record.	
	%% By Default Scylla will not add any new record if both primary and cluster key is same 
	%% So we don't need to check records exist just simple Added the insert statement will work.
	%%%===================================================================
	TSAddinteger = p1_time_compat:system_time(micro_seconds),	
	erlcass:execute(?SET_PRIVACY, [BigIntUser, Default, PLists,TSAddinteger]),
ok.

%%%===================================================================
%%% Last Seen related functions
%%%===================================================================

%%get last activity
get_last(LUser, LServer)->
	
	SelectCols = [{<<"activity">>,text},{<<"lastactivitytime">>,bigint}],	
	%%{BigIntUser,[]} = string:to_integer(binary_to_list(LUser)),	
	BigIntUser = list_to_integer(binary_to_list(LUser)),
	{ok , SelectCols ,Result } =  erlcass:execute(?GET_LAST, ?BIND_BY_NAME, [{<<"userid">>, BigIntUser}]),	
	if (length(Result) > 0 ) ->
			[[Activity, LastActivityTime]] = Result;		
		true->
			Activity = <<"">>,
			LastActivityTime = 0
	end,
{last_activity,{LUser ,LServer},LastActivityTime,Activity}.


%%Store User last activity info
store_last_info(LUser, _LServer, _TimeStamp, Status)->
	%%{BigIntUser,[]} = string:to_integer(binary_to_list(LUser)),
	BigIntUser = list_to_integer(binary_to_list(LUser)),
	TSinteger = p1_time_compat:system_time(micro_seconds),			
	ok = erlcass:execute(?STORE_LAST, [BigIntUser, Status, TSinteger, TSinteger]).


%%%===================================================================
%%% gen_server API
%%%===================================================================
%% @private
init(_Host) ->
	ok.

%% @private
handle_call(get_pid, _From, #state{pid = Pid} = State) ->
    {reply, {ok, Pid}, State};
	
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info({'DOWN', _MonitorRef, _Type, _Object, _Info}, State) ->
    {stop, normal, State};
handle_info(_Info, State) ->
    ?ERROR_MSG("unexpected info: ~p", [_Info]),
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
get_ndb_pid(PoolPid) ->
    case catch gen_server:call(PoolPid, get_pid) of
	{ok, Pid} ->
	    Pid;
	{'EXIT', {timeout, _}} ->
	    throw({error, timeout});
	{'EXIT', Err} ->
	    throw({error, Err})
    end.

%%===================================================================
%%% Sending Push notification to user contacts
%%%===================================================================
send_contact_push(SenderID, LServer,Subscribers, Type, Name, Image, Status, StickersAllowed, SeenReceiptAllowed, SharedWallpapersAllowed, ImageUpdate,Storage,Version) ->	
	
	%% Sending both IOS & Android
	PushURL = gen_mod:get_module_opt(LServer, mod_offline, push_url),
	PayLoad = "sender="++binary_to_list(SenderID)++"&subscriber="++binary_to_list(Subscribers)++"&profiletype="++integer_to_list(Type)++"&type=contact&msg=Contact Updated&service=lynk&name="++binary_to_list(Name)++"&image="++binary_to_list(Image)++"&status="++binary_to_list(Status)++"&stickers="++integer_to_list(StickersAllowed)++"&seenreceipt="++integer_to_list(SeenReceiptAllowed)++"&sharedwallpapers="++integer_to_list(SharedWallpapersAllowed)++"&imageupdate="++binary_to_list(ImageUpdate)++"&imageupdate="++binary_to_list(Storage)++"&imageupdate="++integer_to_list(Version),
	
	PayLoadMsg = PayLoad++"/utf8",
	MsgBytes = byte_size(list_to_binary(PayLoadMsg)),
	if MsgBytes >= 3800 ->
		Complete = "0";
		true->
		Complete = "1"
	end,
	httpc:request(post, {binary_to_list(PushURL), [], "application/x-www-form-urlencoded", list_to_binary(PayLoad++"&is_complete="++Complete)},[],[]).
	
%%===================================================================
%%% Sending Push notification
%%%===================================================================
send_push_message(SenderID, ReceiverID, MsgBody, Type, MetaData,LServer,TS) ->	
	
	%% Sending both IOS & Android
	PushURL = gen_mod:get_module_opt(LServer, mod_offline, push_url),
	PayLoad = "sender="++binary_to_list(SenderID)++"&subscriber="++binary_to_list(ReceiverID)++"&type="++binary_to_list(Type)++"&msg="++http_uri:encode(binary_to_list(MsgBody))++"&metadata="++http_uri:encode(binary_to_list(MetaData))++"&service=lynk&serverid="++binary_to_list(TS),
	PayLoadMsg = PayLoad++"/utf8",
	MsgBytes = byte_size(list_to_binary(PayLoadMsg)),
	if MsgBytes >= 3800 ->
		Complete = "0";
		true->
		Complete = "1"
	end,
	httpc:request(post, {binary_to_list(PushURL), [], "application/x-www-form-urlencoded", list_to_binary(PayLoad++"&is_complete="++Complete)},[],[]).	
	

%%===================================================================
%%% Sending Push notification
%%%===================================================================
send_voice_push_message(SenderID, ReceiverID, MsgBody, Type, MetaData,LServer,TS, Jingle) ->	
	
	%% Sending both IOS & Android
	PushURL = gen_mod:get_module_opt(LServer, mod_offline, push_url),
	PayLoad = "sender="++binary_to_list(SenderID)++"&subscriber="++binary_to_list(ReceiverID)++"&type="++binary_to_list(Type)++"&msg="++http_uri:encode(binary_to_list(MsgBody))++"&metadata="++http_uri:encode(binary_to_list(MetaData))++"&jingle="++http_uri:encode(binary_to_list(Jingle))++"&service=lynk&serverid="++binary_to_list(TS),
	PayLoadMsg = PayLoad++"/utf8",
	MsgBytes = byte_size(list_to_binary(PayLoadMsg)),
	if MsgBytes >= 3800 ->
		Complete = "0";
		true->
		Complete = "1"
	end,
	httpc:request(post, {binary_to_list(PushURL), [], "application/x-www-form-urlencoded", list_to_binary(PayLoad++"&is_complete="++Complete)},[],[]).		
	
%%===================================================================
%%% Parse and store Media Data
%%%===================================================================	
store_media_data(Packet)->
	EncodePacket = xmpp:encode(Packet),
	MetaDataAvailable = fxml:get_subtag(EncodePacket, <<"data">>),
	if MetaDataAvailable =/= false ->
		MetaObj = fxml:get_subtag(EncodePacket, <<"data">>),
		MessageType = fxml:get_tag_attr_s(<<"message_type">>,MetaObj),
		%%===================================================================
		%%% This case will parse the data for Media
		%%%===================================================================	
		#xmlel{children = SubEls} =
		MetaObj,
		ImageList = fxml:remove_cdata(SubEls),
		lists:foreach(fun(N) ->
			case xmpp:get_name(N) == <<"media">> of
			true->
			  MediaURL = fxml:get_tag_cdata(N),
			  [UserID, FileID] = re:split(MediaURL, "/", [{return, list}]),
			  Platform = fxml:get_tag_attr_s(<<"platform">>,N),
			  %%{BigIntUser,[]} = string:to_integer(UserID),	
			  BigIntUser = list_to_integer(UserID),
			  Accessed = trunc(p1_time_compat:system_time(micro_seconds)) div 1000000 ,				  
			  erlcass:execute(?STORE_MEDIA, [BigIntUser, FileID, Platform, MessageType, Accessed]);
			false->
				ok
			end			  
		 end, ImageList);		
	 true->
		ok
end.

%%%===================================================================
%%% Make Muc Options
%%%===================================================================
make_muc_opt(Opts, Title, Description, Affs, Subscribers, GroupID)->
  BinOpts = mod_muc:opts_to_binary(ejabberd_sql:decode_term(Opts)),
  OptsWithTitle = lists:append(BinOpts,[{title,Title}]),
  OptsWithDesc = lists:append(OptsWithTitle,[{description,Description}]),
  OptsWithAffs = lists:append(OptsWithDesc,scylla_to_affiliations(Affs)),
  
  SubList = erlang:is_atom(Subscribers),
  %%===================================================================
		%%% If there is no subcribers so we need to remove these room from the database.
		%%%===================================================================
		ok = erlcass:execute(?DEL_ROOM, ?BIND_BY_NAME, [{<<"groupid">>, GroupID}]),		
		[];
	true->
		lists:append(OptsWithAffs,[{subscribers,scylla_to_subscribers(Subscribers)}])		
  end.

%%%===================================================================
%% Converts db values to ejabberd values. DB has map with (K, {values})
%% We need to convert it into {jid, nick, [namespace values]}
%%%===================================================================
scylla_to_subscribers(SubcribersList) ->
  lists:map(fun ({Jid , Subscriptions}) ->
      SubscriptionList = string:tokens(binary_to_list(Subscriptions), ","),
      JidValue = list_to_binary(integer_to_list(Jid)),	
      {jid:make({JidValue, list_to_binary(?MYHOSTS), <<>>}), JidValue,	
       lists:map(fun(R) ->
	NS_MUCSUB = list_to_binary(R),
	case NS_MUCSUB of 
		?NS_MUCSUB_NODES_SYSTEM_SHORT -> ?NS_MUCSUB_NODES_SYSTEM;
		?NS_MUCSUB_NODES_MESSAGES_SHORT -> ?NS_MUCSUB_NODES_MESSAGES;
		?NS_MUCSUB_NODES_PARTICIPANTS_SHORT -> ?NS_MUCSUB_NODES_PARTICIPANTS;
		?NS_MUCSUB_NODES_AFFILIATIONS_SHORT -> ?NS_MUCSUB_NODES_AFFILIATIONS;
		?NS_MUCSUB_NODES_PRESENCE_SHORT -> ?NS_MUCSUB_NODES_PRESENCE;
		?NS_MUCSUB_NODES_SUBJECT_SHORT -> ?NS_MUCSUB_NODES_SUBJECT;
		?NS_MUCSUB_NODES_CONFIG_SHORT -> ?NS_MUCSUB_NODES_CONFIG
		end	
	end, SubscriptionList)}
  end, SubcribersList).
 
%%%===================================================================
%% Converts db values to ejabberd values. DB has map with (UserID,"Affiliation")
%% We need to convert it into {{<<userid>>,<<host>>,<<Resource>>},{member,<<Reason>>}}
%%%===================================================================
 
scylla_to_affiliations(Affs) ->
	lists:map(
		fun({U, Aff}) ->
			{{integer_to_list(U), list_to_binary(?MYHOSTS), <<>>},{misc:binary_to_atom(Aff), <<>>}}
  end, Affs).
  
 %%%===================================================================
%% Converts ejabberd values to scyllaDB map. DB has {jid, nick, [namespace values]}
%% and as nick and jid are same, so we will save the map as with (K, values)
%%%===================================================================
subcribers_to_scylla(SubsciptionTuple) ->
          
    lists:map(
      % Traverse through all of the subscribers 1st.
      fun({JidTuple, _Nick, SubList}) ->        
        %Add 1st element as the map key (userid and followed by subcriptions.)
        { list_to_integer(binary_to_list(JidTuple#jid.luser)),
        % 1st element is JId, while the rest are list of subscriptions like
        % [JID, subcription1, subcription2 ...] in shorter form
        % [+923213333, <<"system, messages, participatns">>]
        lists:foldr(
			fun(?NS_MUCSUB_NODES_SYSTEM, NS) ->
					<<?NS_MUCSUB_NODES_SYSTEM_SHORT/binary, <<",">>/binary, NS/binary>>;
				(?NS_MUCSUB_NODES_MESSAGES, NS) ->
					<<?NS_MUCSUB_NODES_MESSAGES_SHORT/binary, <<",">>/binary, NS/binary>>;
				(?NS_MUCSUB_NODES_PARTICIPANTS, NS) ->
					<<?NS_MUCSUB_NODES_PARTICIPANTS_SHORT/binary, <<",">>/binary, NS/binary>>;
				(?NS_MUCSUB_NODES_AFFILIATIONS, NS) ->
					<<?NS_MUCSUB_NODES_AFFILIATIONS_SHORT/binary, <<",">>/binary, NS/binary>>;
				(?NS_MUCSUB_NODES_PRESENCE, NS) ->
					<<?NS_MUCSUB_NODES_PRESENCE_SHORT/binary, <<",">>/binary, NS/binary>>;
				(?NS_MUCSUB_NODES_SUBJECT, NS) ->
					<<?NS_MUCSUB_NODES_SUBJECT_SHORT/binary, <<",">>/binary, NS/binary>>;
				(?NS_MUCSUB_NODES_CONFIG, NS) ->
					<<?NS_MUCSUB_NODES_CONFIG_SHORT/binary, <<",">>/binary, NS/binary>>
          end, <<>>,  SubList)}
      end,  SubsciptionTuple). 

%%%===================================================================
%% Converts ejabberd values to scyllaDB map. DB has {{<<userid>>,<<host>>,<<Resource>>},{member,<<Reason>>}}
%% We will save the value like [{userid, "member"},...]
%%%===================================================================
affiliations_to_scylla(Affs) ->
	lists:map( fun({{U, _H, _Res}, {Aff, _Reason}}) ->
		{list_to_integer(binary_to_list(U)), misc:atom_to_binary(Aff)};
			({{U, _H, _Res}, Aff}) ->
		{list_to_integer(binary_to_list(U)), misc:atom_to_binary(Aff)}
	end, Affs).
