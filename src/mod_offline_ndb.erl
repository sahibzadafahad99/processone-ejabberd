%%%-------------------------------------------------------------------
%%% File    : mod_offline_ndb.erl
%%% Author  : Muhmammad Naeem <m.naemakram@gmail.com>
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
%%%----------------------------------------------------------------------

-module(mod_offline_ndb).

-behaviour(mod_offline).

-export([init/2, store_message/1, pop_messages/2, remove_expired_messages/1,
	 remove_old_messages/2, remove_user/2, read_message_headers/2,
	 read_message/3, remove_message/3, read_all_messages/2,
	 remove_all_messages/2, count_messages/2, import/1]).

-include("xmpp.hrl").
-include("mod_offline.hrl").
-include("logger.hrl").

%%%===================================================================
%%% API
%%%===================================================================
init(_Host, _Opts) ->
    ok.

%%store_message(#offline_msg{us = {LUser, LServer}} = M) ->
store_message(#offline_msg{us = {_, LServer}} = M) ->
    From = M#offline_msg.from,
    To = M#offline_msg.to,
    Packet = xmpp:set_from_to(M#offline_msg.packet, From, To),
	TS = p1_time_compat:system_time(micro_seconds),
	TSinteger = trunc(p1_time_compat:system_time(micro_seconds)) div 1000 ,
	TimeStamp = list_to_binary(integer_to_list(TS)),
	%%%===================================================================
	%% Sir Zeeehan said no need delay tag. 31/10/2017	
	%% NewPacket = xmpp_util:add_delay_info(Packet, jid:make(LServer), M#offline_msg.timestamp, <<"Offline Storage">>),
	%% As we don't want to store the XML 
	%% So we don't need to convert the packet in binary format.
    %% XML = fxml:element_to_binary( xmpp:encode(NewPacket)),
	%%%===================================================================
	EncodePacket = xmpp:encode(Packet),
	ValidMessage = isValidateTextMessage(EncodePacket),
	Type = get_message_type(EncodePacket),	
	MsgID = get_message_id(EncodePacket),	
	XML = fxml:element_to_binary(EncodePacket),
	if ValidMessage == <<"message">> ->	
		%%%===================================================================
		%% As we are creating XML Stanza run time that's why we need to store complete sender Address
		%% e.g groupName@conference.145.239.135.77
		%%%===================================================================
		SenderID = list_to_binary(binary_to_list(From#jid.luser)++"@"++binary_to_list(From#jid.lserver)),
		ValidGroupMessage = isValidateGroupMessage(XML),
		ReceiverID = To#jid.luser,		
		if (Type == <<"">>) or (Type == <<"normal">>) ->
			IsInvitation = isInvitationMessage(EncodePacket),
			NewMsgID = if IsInvitation ->
				MyType = <<"normal">>,
				MsgBody = fxml:get_subtag_cdata(EncodePacket, <<"body">>),	
				MetaData = fxml:element_to_binary(fxml:get_subtag(EncodePacket, <<"x">>)),
				ejabberd_ndb:send_push_message(SenderID, ReceiverID, MsgBody, MyType, MetaData,LServer,TimeStamp,MsgID),
				list_to_binary("9200000000_"++integer_to_list(TSinteger));
			true->
				_isGroupChat = string:str(binary_to_list(XML), "type='groupchat'"),				
				if(	_isGroupChat >= 1) ->
					MyType = <<"groupchat">>,
					MetaData = fxml:element_to_binary(fxml:get_subtag(EncodePacket, <<"event">>)),					
					NewMsgPacket = get_muc_message_stanza(EncodePacket),		
					NewPacket = xmpp:encode(NewMsgPacket),					
					MsgBody = fxml:get_subtag_cdata(NewPacket, <<"body">>),						
					NewSenderID = get_message_from(NewPacket),					
					if (ValidGroupMessage == <<"message">>) ->						
						PushMetaData = fxml:element_to_binary(fxml:get_subtag(NewPacket, <<"data">>)),					
						ejabberd_ndb:send_push_message(NewSenderID, ReceiverID, MsgBody, MyType, PushMetaData,LServer,TimeStamp,MsgID);
					true->
						ok
					end,
					get_message_id(NewPacket);					
				true ->
					MyType = <<"normal">>,
					MsgBody = fxml:get_subtag_cdata(EncodePacket, <<"body">>),	
					MetaData = <<"">>,
					ejabberd_ndb:send_push_message(SenderID, ReceiverID, MsgBody, MyType, MetaData,LServer,TimeStamp,MsgID),
					list_to_binary("9200000000_"++integer_to_list(TSinteger))
				end
			end,
			if ValidGroupMessage == <<"message">> ->	
				ejabberd_ndb:store_offline_message(SenderID,ReceiverID, MsgBody, MetaData, MyType, NewMsgID,TS);
			true->
				ok
			end;
		true->			
			MsgBody = fxml:get_subtag_cdata(EncodePacket, <<"body">>),
			MetaData = fxml:element_to_binary(fxml:get_subtag(EncodePacket, <<"data">>)),
			MyType = isValidateChatMessageType(EncodePacket),
			MsgBodyText = if (MyType == <<"received">>) or (MyType == <<"displayed">>) ->
							MsgID;
						true->
						ejabberd_ndb:send_push_message(SenderID, ReceiverID, MsgBody, MyType, MetaData,LServer, TimeStamp, MsgID),
						MsgBody
					end,
			ejabberd_ndb:store_offline_message(SenderID,ReceiverID, MsgBodyText, MetaData, MyType, MsgID, TS)			
		end;
	true->
		ok
	end,
	ok.

	
pop_messages(LUser, LServer ) ->
	Rs = ejabberd_ndb:get_offline_messages(LUser, LServer),
	{ok, lists:map(fun (R) ->
	    [Senderid, Messagetext, Metadata, Type , _Createddate, MsgID] = R,		
		%%%===================================================================
		%%% Mod Offline related functions
		%%% Sir Zeeehan said no need delay tag. 31/10/2017
		%%% In case of Invite we will save the X tag complete in the metaData 
		%%%===================================================================
		if (Type == <<"">>) or (Type == <<"normal">>) ->		
			MyMessage = "<message type='"++binary_to_list(Type)++"' to='"++binary_to_list(LUser)++"@"++binary_to_list(LServer)++"' from='"++binary_to_list(Senderid)++"' id='"++binary_to_list(MsgID)++"'>				<body>"++binary_to_list(Messagetext)++"</body>"++binary_to_list(Metadata)++"</message>";		
		true->			
			if  (Type == <<"received">>) or (Type == <<"displayed">>) ->
				MyMessage = "<message xmlns='jabber:client' lang='en' type='chat' to='"++binary_to_list(LUser)++"@"++binary_to_list(LServer)++"' from='"++binary_to_list(Senderid)++"' id='"++binary_to_list(MsgID)++"'><"++binary_to_list(Type)++" xmlns='urn:xmpp:chat-markers:0' id='"++binary_to_list(Messagetext)++"'></"++binary_to_list(Type)++">"++binary_to_list(Metadata)++"</message>";
			true->
				if Type == <<"chat">> ->
					MyMessage = "<message xmlns='jabber:client' lang='en' type='"++binary_to_list(Type)++"' to='"++binary_to_list(LUser)++"@"++binary_to_list(LServer)++"' from='"++binary_to_list(Senderid)++"' id='"++binary_to_list(MsgID)++"'><body>"++binary_to_list(Messagetext)++"</body>"++binary_to_list(Metadata)++"<request>urn:xmpp:receipts</request><markable xmlns='urn:xmpp:chat-markers:0'></markable></message>";
				 true->	
					MyMessage = "<message to='"++binary_to_list(LUser)++"@"++binary_to_list(LServer)++"' from='"++binary_to_list(Senderid)++"'>"++binary_to_list(Metadata)++"</message>"				
				end
			end
		end,		
		MsgText = list_to_binary(MyMessage),		
        case xml_to_offline_msg(MsgText) of
			{ok, Msg} ->
			Msg;
		_Err ->
		[]
	end
end, Rs)}.
	

count_messages(LUser, LServer) ->
    case ejabberd_ndb:get_offline_messages(LUser ,LServer) of
        {ok, Res} ->
            Res;
        _ ->
            0
    end.
	
remove_expired_messages(_LServer) ->
    %% TODO
    {atomic, ok}.

remove_old_messages(_Days, _LServer) ->
    %% TODO
    {atomic, ok}.

remove_user(_LUser, _LServer) ->
    %% TODO
    {atomic, ok}.

read_message_headers(LUser, LServer) ->
    [].

read_message(_LUser, _LServer, _I) ->
	%% TODO
    ok.

remove_message(_LUser, _LServer, _I) ->
    %% TODO
    ok.
remove_all_messages(_LUser, _LServer) ->
    %% TODO
    {atomic, ok}.
	
read_all_messages(_LUser, _LServer) ->
    [].
import(_) ->
    %% TODO
    {atomic, ok}.
	
%%%===================================================================
%%% Internal functions
%%%===================================================================
	
xml_to_offline_msg(XML) ->
    case fxml_stream:parse_element(XML) of
	#xmlel{} = El ->
	    el_to_offline_msg(El);
	Err ->
	    ?ERROR_MSG("got ~p when parsing XML packet ~s",
		       [Err, XML]),
	    Err
    end.

el_to_offline_msg(El) ->
    To_s = fxml:get_tag_attr_s(<<"to">>, El),
    From_s = fxml:get_tag_attr_s(<<"from">>, El),
    try
	To = jid:decode(To_s),
	From = jid:decode(From_s),
	{ok, #offline_msg{us = {To#jid.luser, To#jid.lserver},
			  from = From,
			  to = To,
			  packet = El}}
    catch _:{bad_jid, To_s} ->
	    ?ERROR_MSG("failed to get 'to' JID from offline XML ~p", [El]),
	    {error, bad_jid_to};
	  _:{bad_jid, From_s} ->
	    ?ERROR_MSG("failed to get 'from' JID from offline XML ~p", [El]),
	    {error, bad_jid_from}
    end.

%%===================================================================
%%% Fetch MUC Message Stanza
%%%===================================================================
get_muc_message_stanza(#xmlel{name = <<"message">>, children = Els}) ->
_items = lists:foldl(fun (#xmlel{name = <<"event">>} =
			 El,
		     false) ->
			fxml:get_subtag(El, <<"items">>);
		    (_, Acc) -> Acc
		end,
		false, Els),
_item = fxml:get_subtag(_items, <<"item">>),
_message = fxml:get_subtag(_item, <<"message">>),
_message.
	
	
%%===================================================================
%%% Check weather message is invite or not
%%%===================================================================
	
isInvitationMessage(#xmlel{name = <<"message">>, children = Els}) ->
    lists:foldl(fun (#xmlel{name = <<"x">>, attrs = Attrs} =
			 El,
		     false) ->
			case fxml:get_attr_s(<<"xmlns">>, Attrs) of
			  ?NS_MUC_USER ->
			      case fxml:get_subtag(El, <<"invite">>) of
				false -> false;
				_ -> true
			      end;
			  _ -> false
			end;
		    (_, Acc) -> Acc
		end,
		false, Els).
		
%%===================================================================
%%% Veriry messate type as we are creating stanza on the run so we need to know correct type of stanza
%%%===================================================================

isValidateChatMessageType(Packet)-> 
  ReceivedMarker = fxml:get_subtag(Packet, <<"received">>),
  DisplayedMarker = fxml:get_subtag(Packet, <<"displayed">>),
  MsgType = if ReceivedMarker =/= false ->
		<<"received">>;
   true->   	
	if DisplayedMarker =/= false ->
		<<"displayed">>;
	 true->   	
		  <<"chat">>				
	end	
  end,
MsgType.

%%===================================================================
%%% Verify that the message has only data not the composing or any chat State
%%% If its a composing or paused message then we don't need to store in the spool / offline table
%%%===================================================================

isValidateGroupMessage(XML)->  
  AcknowledgedMarker = string:str(binary_to_list(XML), "acknowledged"),
  ComposingMarker = string:str(binary_to_list(XML), "composing"),
  PausedMarker = string:str(binary_to_list(XML), "paused"),
  
  ChatState = if( AcknowledgedMarker >= 1) -> 
	<<"acknowledged">>;
   true->   	
	if( ComposingMarker >= 1) -> 
		<<"composing">>;
	 true->   	
		if( PausedMarker >= 1) -> 
			<<"paused">>;	
		true->  
			<<"message">>
		end		
	end	
  end,
ChatState.

%%===================================================================
%%% Verify that the message has only data not the composing or any chat State
%%% If its a composing or paused message then we don't need to store in the spool / offline table
%%%===================================================================

isValidateTextMessage(Packet)->  
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


%%===================================================================
%%% Find message From 
%%%===================================================================
-spec get_message_from(xmlel()) -> binary().

get_message_from(#xmlel{attrs = Attrs}) ->
    fxml:get_attr_s(<<"from">>, Attrs).
	
	
%%===================================================================
%%% Find message type 
%%%===================================================================
-spec get_message_id(xmlel()) -> binary().

get_message_id(#xmlel{attrs = Attrs}) ->
    fxml:get_attr_s(<<"id">>, Attrs).
	
-spec get_message_type(xmlel()) -> binary().
get_message_type(#xmlel{attrs = Attrs}) ->
    case fxml:get_attr_s(<<"type">>, Attrs) of
      <<"">> ->
		<<"normal">>;
      Type ->
		Type
    end.
	

	
