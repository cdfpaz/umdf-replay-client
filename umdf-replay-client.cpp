// Copyright (c) 2012, Clebson Derivan ( cderivan@gmail.com ).
// All rights reserved.
//
// the mensages implemented here follow the specification described
// on document UMDF FIX/FAST Message Reference - version 1.6.3
// for more information:
// http://www.bmfbovespa.com.br/en-us/services/trading-platforms/puma-trading-system/Market-Data-Feed.asp

#include <quickfix/Application.h>
#include <quickfix/FileLog.h>
#include <quickfix/FileStore.h>
#include <quickfix/SocketInitiator.h>
#include <quickfix/SessionSettings.h>

#include <iostream>
#include <fstream>
#include <boost/program_options.hpp>

#include <Application/QuickFAST.h>
#include <Codecs/Decoder.h>
#include <Codecs/TemplateRegistry.h>
#include <Codecs/DataSourceBuffer.h>

#include "ValueToFix.h"

using namespace std;

QuickFAST::Codecs::Decoder* decoder;
QuickFAST::Examples::ValueToFix* builder_real;

//define FIX Api implementation
class FixApi : public FIX::Application {
public:
	FixApi(const std::string& c, const std::string& b, const std::string& e)
		: channel(c)
		, begin_seq(b)
		, end_seq(e)
	{
		  //template provided by BM&FBOVESPA
		  fast_template_file.open( "templates-UMDF-NTP.xml", ios::in | ios::binary );
		  cout << "parsing umdf template file" << endl;

		  QuickFAST::Codecs::XMLTemplateParser parser;
		  QuickFAST::Codecs::TemplateRegistryPtr tmpl=parser.parse(fast_template_file);
		  cout << "umdf template file parsed" << endl;

		  decoder = new QuickFAST::Codecs::Decoder( tmpl );
		  builder_real = new QuickFAST::Examples::ValueToFix( std::cout );
	}

	void run();

private:
	void onCreate( const FIX::SessionID& ) {}

	void fromAdmin( const FIX::Message&, const FIX::SessionID& ) 
		throw( FIX::FieldNotFound, FIX::IncorrectDataFormat, FIX::IncorrectTagValue, FIX::RejectLogon ) {}

	void toAdmin( FIX::Message&, const FIX::SessionID& ) {}

	void onLogon( const FIX::SessionID& sessionID );
	void onLogout( const FIX::SessionID& sessionID ) {}
	void toApp( FIX::Message&, const FIX::SessionID& ) throw( FIX::DoNotSend ) {}

	void fromApp( const FIX::Message& message, const FIX::SessionID& sessionID ) 
		throw( FIX::FieldNotFound, FIX::IncorrectDataFormat, FIX::IncorrectTagValue, FIX::UnsupportedMessageType );

private:
	std::string channel;
	std::string begin_seq;
	std::string end_seq;

	ifstream fast_template_file;
};

//define repeating group
class NoApplIDs: public FIX::Group {
public:
	NoApplIDs() : FIX::Group(1351,1355,FIX::message_order(1355,1182,1183,0)) {}
};

void FixApi::onLogon( const FIX::SessionID& sessionID ) {
	FIX::Message request;
	request.getHeader().setField( FIX::MsgType( "BW" ) );
	request.setField( 1346, "RequestId"  );
	request.setField( 1347, "0"  );
	
	NoApplIDs group;
	group.setField( 1355, channel  );
	group.setField( 1182, begin_seq  );
	group.setField( 1183, end_seq  );
	request.addGroup( group );
	
	std::cout << "connected, sending FIX request to channel: " << channel << " begin msg: " << begin_seq << " end msg: " << end_seq << std::endl;
	FIX::Session::sendToTarget(request, sessionID);
}

void FixApi::run() {
	std::cout << "dude, press enter to stop it (:" << std::endl;
	std::cin.get();
}

//the magic
void FixApi::fromApp( const FIX::Message& message, const FIX::SessionID& sessionID ) 
	throw( FIX::FieldNotFound, FIX::IncorrectDataFormat, FIX::IncorrectTagValue, FIX::UnsupportedMessageType ) {

	const std::string& msgTypeValue = message.getHeader().getField( FIX::FIELD::MsgType );
	if ( msgTypeValue == "BX" ) {
		const std::string& reqResponse = message.getField( 1348 );	
		if ( reqResponse == "0" ) {
			std::cout << "Request accepted" << std::endl;
		} 
		else if ( reqResponse == "1" ) {
			std::cout << "Application does not exist (not specified)" << std::endl;
		}
		else if ( reqResponse == "2" ) {
			FIX::Group grp(1351, 1355);
			message.getGroup(1, grp);
			const std::string& errorResponse = grp.getField( 1354 );

			std::string errorDesc("unknown");

			if ( errorResponse == "0" ) {
				errorDesc = "Application does not exist";
			}
			else if ( errorResponse == "3") {
				errorDesc = "Invalid range requested";
			}

			std::cout << "Request not accepted, reason: " << errorDesc << std::endl;
		}
		else if ( reqResponse == "6" ) {
			std::cout << "Not authorized" << std::endl;
		}
	}
	else if ( msgTypeValue == "BY" ) {
		const std::string& reqResponse = message.getField( 1426 );
		if ( reqResponse == "3" ) {
			std::cout << "all messages received, no errors found" << std::endl;
		}
		else if ( reqResponse == "4" ) {

			FIX::Group grp(1351, 1355);
			message.getGroup(1, grp);
			const std::string& errorResponse = grp.getField( 1354 );

			std::string errorDesc("unknown");

			if ( errorResponse == "1" ) {
				errorDesc = "Messages requested are not available";
			}
			else if ( errorResponse == "4") {
				errorDesc = "Exceeded the maximum limit of messages allowed per resend request";
			}
			else if ( errorResponse == "5") {
				errorDesc = "Exceeded the maximum limit of messages allowed per resend request";
			}
			else if ( errorResponse == "6") {
				errorDesc = "ending Bottom-N messages of the requested range";
			}
			else if ( errorResponse == "7") {
				errorDesc 
					= "Exceeded the maximum limit of messages allowed per resend request and ending Bottom-N messages of the requested range";
			}
			std::cout << "server returned error: " << errorDesc << std::endl;
		}
	}
	else if ( msgTypeValue == "URDR" ) {
		std::cout << "received raw data reporting";

		FIX::RawData rawData;
		FIX::RawDataLength rawDataLength;

		message.getField( rawData );
		message.getField( rawDataLength );

		const char* rawBytes = rawData.getValue().c_str();

		int grpCount = atoi(message.getField( 10054 ).c_str());
		std::cout << " number of messages in this report: " << grpCount  << std::endl;

		for( int i = 1; i < grpCount; i++ ) {
			FIX::Group grp(10054, 1181);
			message.getGroup(i, grp);

			int msg_offset = atoi(grp.getField( 10055 ).c_str());
			FIX::RawDataLength msg_lenght;
			grp.getField(msg_lenght);

			std::string rawMsg;
			rawMsg.assign( (const char*)&rawBytes[msg_offset], msg_lenght.getValue() );

			QuickFAST::Codecs::DataSourceBuffer buff( (const unsigned char*)rawMsg.c_str(), msg_lenght.getValue() );
			decoder->decodeMessage( buff, *builder_real );
		}
	}
	else
		std::cout << "received unknown message: " << message.toString() << std::endl;
}


using namespace boost::program_options;

int main( int argc, char* argv[] ) 
{
	options_description options("Allowed options");
	options.add_options()
	    ("config,f", value< std::string >(), "FIX config file" )
	    ("channel,c", value< std::string >(), "PUMA Channel" )
	    ("beginseq,b", value< std::string >(), "begin sequence number"  )
	    ("endseq,e", value< std::string >(), "end sequence number"  )
	    ;

	variables_map vm;
	try {
	    store(parse_command_line(argc, argv, options), vm);
	    notify(vm);
	}
	catch (const std::exception& ex)
	{
	    std::cout << "Invalid configuration: " << ex.what() << std::endl;
	    std::cout << options << std::endl;
	    return 1;
	}

	if (!vm.count("config"))
	{
	    std::cout << "configuration file not informed"<< std::endl;
	    std::cout << options << std::endl;
	    return 1;
	}

	if (!vm.count("channel"))
	{
	    std::cout << "channel was not informed, please review configuration"<< std::endl;
	    std::cout << options << std::endl;
	    return 1;
	}

	if (!vm.count("beginseq") || !vm.count("endseq") )
	{
	    std::cout << "both begin sequence number and sequence number are mandatory"<< std::endl;
	    std::cout << options << std::endl;
	    return 1;
	}

	std::string channel = vm["channel"].as< std::string >();
	std::string begin_seq = vm["beginseq"].as< std::string >();
	std::string end_seq = vm["endseq"].as< std::string >();
	std::string file = vm["config"].as< std::string >();

	try
	{
		FIX::SessionSettings settings( file );

		FixApi application(channel, begin_seq, end_seq);
		FIX::FileLogFactory logFactory( settings );
		FIX::FileStoreFactory storeFactory( settings );
		FIX::SocketInitiator initiator( application, storeFactory, settings, logFactory );
		
		std::cout << "connecting..." << std::endl;
		initiator.start();
		application.run();
		initiator.stop();

		return 0;
	}
	catch ( std::exception & e )
	{
		std::cout << e.what();
		return 1;
	}
}
