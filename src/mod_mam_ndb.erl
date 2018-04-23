%%%-------------------------------------------------------------------
%%% File    : mod_mam_ndb.erl
%%% Author  : Muhammad Naeem <m.naemakram@gmail.com>
%%% Created : 7 Aug 2017 by Muhammad Naeem <m.naemakram@gmail.com>
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
%%%----------------------------------------------------------------------

-module(mod_mam_ndb).

-behaviour(mod_mam).

%% API
-export([init/2, remove_user/2, remove_room/3, delete_old_messages/3,
	 extended_fields/0, store/8, write_prefs/4, get_prefs/2, select/6]).

-include_lib("stdlib/include/ms_transform.hrl").
-include("xmpp.hrl").
-include("logger.hrl").
-include("mod_mam.hrl").

-define(BIN_GREATER_THAN(A, B),
	((A > B andalso byte_size(A) == byte_size(B))
	 orelse byte_size(A) > byte_size(B))).
-define(BIN_LESS_THAN(A, B),
	((A < B andalso byte_size(A) == byte_size(B))
	 orelse byte_size(A) < byte_size(B))).

-define(TABLE_SIZE_LIMIT, 2000000000). % A bit less than 2 GiB.

%%%===================================================================
%%% API
%%%===================================================================
init(_Host, _Opts) ->
    ok.

remove_user(_LUser, _LServer) ->
	%% TO DO
    ok.

remove_room(_LServer, LName, LHost) ->
    remove_user(LName, LHost).

delete_old_messages(global, _TimeStamp, _Type) ->
	%% TO DO
    ok.

extended_fields() ->
    [].

store(Pkt, _LServer, {LUser, LHost}, Type, _Peer, _Nick, _Dir, TS) ->

	TSinteger = p1_time_compat:system_time(micro_seconds),
    %ID = jlib:integer_to_binary(TSinteger),    
	%%XML = fxml:element_to_binary(Pkt),
    MsgBody = fxml:get_subtag_cdata(Pkt, <<"body">>),	
	MetaDataAvailable = fxml:get_subtag(Pkt, <<"data">>),
	MetaData = if MetaDataAvailable =/= false ->
					fxml:element_to_binary(fxml:get_subtag(Pkt, <<"data">>));
				true->
					<<"acknowledged">>
				end, 

	SType = misc:atom_to_binary(Type),
	SUser = case Type of
		chat -> LUser;
		groupchat -> jid:encode({LUser, LHost, <<>>})
	    end,
		
	SenderID = jid:encode({LUser, LHost, <<>>}),	
	ValidMessage = isValidateTextMessage(Pkt),	
	if ValidMessage == <<"message">> ->
		if _Dir == send  ->
			ejabberd_ndb:store_archive(SType, SenderID, SUser, MsgBody ,MetaData);
		true->
			if Type == groupchat  ->
				ejabberd_ndb:store_archive(SType, SenderID, SUser, MsgBody ,MetaData);
			true->
				{ok,<<"0">>}
			end
		end;
	true->
		{ok,<<"0">>}
	end.
	

write_prefs(_LUser, _LServer, _Prefs, _ServerHost) ->
   %% To DO
   ok.

get_prefs(_LUser, _LServer) ->
	PrefData = [],
    case PrefData of
	[Prefs] ->
	    {ok, Prefs};
	_ ->
	    error
    end.

%%%===================================================================
%%% This function need to be done in future 
%%% Currently we don't need this but if required than we have to modify it.
%%%===================================================================
select(_LServer, JidRequestor,
       #jid{luser = LUser, lserver = LServer} = JidArchive,
       Query, RSM, MsgType) ->
    Start = proplists:get_value(start, Query),
    End = proplists:get_value('end', Query),
    With = proplists:get_value(with, Query),
    LWith = if With /= undefined -> jid:tolower(With);
	       true -> undefined
	    end,
    MS = make_matchspec(LUser, LServer, Start, End, LWith),
    Msgs = ndb:dirty_select(archive_msg, MS),
    SortedMsgs = lists:keysort(#archive_msg.timestamp, Msgs),
    {FilteredMsgs, IsComplete} = filter_by_rsm(SortedMsgs, RSM),
    Count = length(Msgs),
    Result = {lists:flatmap(
		fun(Msg) ->
			case mod_mam:msg_to_el(
			       Msg, MsgType, JidRequestor, JidArchive) of
			    {ok, El} ->
				[{Msg#archive_msg.id,
				  binary_to_integer(Msg#archive_msg.id),
				  El}];
			    {error, _} ->
				[]
			end
		end, FilteredMsgs), IsComplete, Count},
    erlang:garbage_collect(),
    Result.

%%%===================================================================
%%% Internal functions
%%%===================================================================
%now_to_usec({MSec, Sec, USec}) ->
%    (MSec*1000000 + Sec)*1000000 + USec.

make_matchspec(LUser, LServer, Start, undefined, With) ->
    %% List is always greater than a tuple
    make_matchspec(LUser, LServer, Start, [], With);
make_matchspec(LUser, LServer, Start, End, {_, _, <<>>} = With) ->
    ets:fun2ms(
      fun(#archive_msg{timestamp = TS,
		       us = US,
		       bare_peer = BPeer} = Msg)
	    when Start =< TS, End >= TS,
		 US == {LUser, LServer},
		 BPeer == With ->
	      Msg
      end);
make_matchspec(LUser, LServer, Start, End, {_, _, _} = With) ->
    ets:fun2ms(
      fun(#archive_msg{timestamp = TS,
		       us = US,
		       peer = Peer} = Msg)
	    when Start =< TS, End >= TS,
		 US == {LUser, LServer},
		 Peer == With ->
	      Msg
      end);
make_matchspec(LUser, LServer, Start, End, undefined) ->
    ets:fun2ms(
      fun(#archive_msg{timestamp = TS,
		       us = US,
		       peer = Peer} = Msg)
	    when Start =< TS, End >= TS,
		 US == {LUser, LServer} ->
	      Msg
      end).

filter_by_rsm(Msgs, undefined) ->
    {Msgs, true};
filter_by_rsm(_Msgs, #rsm_set{max = Max}) when Max < 0 ->
    {[], true};
filter_by_rsm(Msgs, #rsm_set{max = Max, before = Before, 'after' = After}) ->
    NewMsgs = if is_binary(After), After /= <<"">> ->
		      lists:filter(
			fun(#archive_msg{id = I}) ->
				?BIN_GREATER_THAN(I, After)
			end, Msgs);
		 is_binary(Before), Before /= <<"">> ->
		      lists:foldl(
			fun(#archive_msg{id = I} = Msg, Acc)
				when ?BIN_LESS_THAN(I, Before) ->
				[Msg|Acc];
			   (_, Acc) ->
				Acc
			end, [], Msgs);
		 is_binary(Before), Before == <<"">> ->
		      lists:reverse(Msgs);
		 true ->
		      Msgs
	      end,
    filter_by_max(NewMsgs, Max).

filter_by_max(Msgs, undefined) ->
    {Msgs, true};
filter_by_max(Msgs, Len) when is_integer(Len), Len >= 0 ->
    {lists:sublist(Msgs, Len), length(Msgs) =< Len};
filter_by_max(_Msgs, _Junk) ->
    {[], true}.


%%-spec get_message_id(xmlel()) -> binary().
%%get_message_id(#xmlel{attrs = Attrs}) ->
%%   fxml:get_attr_s(<<"id">>, Attrs).



%%===================================================================
%%% Verify that the message has only data not the composing or any chat State
%%% If its a composing or paused message then we don't need to store in the spool / offline table
%%%===================================================================

isValidateTextMessage(Packet)->  
  %ReceivedMarker = fxml:get_subtag(Packet, <<"received">>),
  %DisplayedMarker = fxml:get_subtag(Packet, <<"displayed">>),
  AcknowledgedMarker = fxml:get_subtag(Packet, <<"acknowledged">>),
  ComposingMarker = fxml:get_subtag(Packet, <<"composing">>),
  PausedMarker = fxml:get_subtag(Packet, <<"paused">>),
  
  ChatState = if AcknowledgedMarker =/= false ->
	<<"acknowledged">>;
   true->   	
	if ComposingMarker =/= false ->
		<<"composing">>;
	 true->   	
		if PausedMarker =/= false ->
			<<"paused">>;	
		true->  
			<<"message">>
		end		
	end	
  end,
ChatState.