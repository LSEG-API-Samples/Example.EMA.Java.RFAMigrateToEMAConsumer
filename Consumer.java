///*|----------------------------------------------------------------------------------------------------
// *|            This source code is provided under the Apache 2.0 license      	--
// *|  and is provided AS IS with no warranty or guarantee of fit for purpose.  --
// *|                See the project's LICENSE.md for details.                  					--
// *|           Copyright (C) 2019 Refinitiv. All rights reserved.            		--
///*|----------------------------------------------------------------------------------------------------

package com.refinitiv.ema.examples.training.consumer.series300.example330__Login__Streaming;

import com.refinitiv.ema.access.Msg;
import com.refinitiv.ema.access.AckMsg;
import com.refinitiv.ema.access.GenericMsg;
import com.refinitiv.ema.access.RefreshMsg;
import com.refinitiv.ema.access.ReqMsg;
import com.refinitiv.ema.access.StatusMsg;
import com.refinitiv.ema.access.UpdateMsg;
import com.refinitiv.ema.access.Data;
import com.refinitiv.ema.access.DataType;
import com.refinitiv.ema.access.DataType.DataTypes;
import com.refinitiv.ema.access.ElementEntry;
import com.refinitiv.ema.access.ElementList;
import com.refinitiv.ema.access.Vector;
import com.refinitiv.ema.access.VectorEntry;
import com.refinitiv.ema.rdm.EmaRdm;
import com.refinitiv.ema.access.EmaFactory;
import com.refinitiv.ema.access.FieldEntry;
import com.refinitiv.ema.access.FieldList;
import com.refinitiv.ema.access.OmmConsumer;
import com.refinitiv.ema.access.OmmConsumerClient;
import com.refinitiv.ema.access.OmmConsumerEvent;
import com.refinitiv.ema.access.OmmException;

class AppClient implements OmmConsumerClient
{
    public void onRefreshMsg(RefreshMsg refreshMsg, OmmConsumerEvent event)
    {
        switch(refreshMsg.domainType()) {
            case EmaRdm.MMT_LOGIN://to handle Login Events which are similar to Connection Events
                System.out.println("Received LOGIN RefreshMsg: ");
                System.out.println("State= " + refreshMsg.state());
                break;
            case EmaRdm.MMT_DIRECTORY://to handle Source Directory Events which are similar to Market Data Service Events
                System.out.println("Received DIRECTORY RefreshMsg: ");
                System.out.println(refreshMsg);
                break;
            case EmaRdm.MMT_MARKET_PRICE://to handle Market Price Events which are similar to Market Data Subscription Events
                System.out.println("Received MARKET_PRICE RefreshMsg:");
                System.out.println("Service = " + refreshMsg.serviceName() + ", item = " + refreshMsg.name() );
                System.out.println("Permission Data " + (refreshMsg.hasPermissionData()?"exists.":"does not exist.") );
                System.out.println("State = " + refreshMsg.state());
                if (DataType.DataTypes.FIELD_LIST == refreshMsg.payload().dataType())
                    decode(refreshMsg.payload().fieldList());
                break;
        }

    }

    public void onUpdateMsg(UpdateMsg updateMsg, OmmConsumerEvent event)
    {
        switch(updateMsg.domainType()) {
            case EmaRdm.MMT_DIRECTORY://to handle Source Directory Events which are similar to Market Data Service Events
                System.out.println("Received DIRECTORY UpdateMsg: ");
                System.out.println(updateMsg);
                break;
            case EmaRdm.MMT_MARKET_PRICE://to handle Market Price Events which are similar to Market Data Subscription Events
                System.out.println("Received MARKET_PRICE UpdateMsg:");
                System.out.println("Service = " + updateMsg.serviceName() + ", item = " + updateMsg.name() );
                System.out.println("Permission Data " + (updateMsg.hasPermissionData()?"exists.":"does not exist.") );

                if (DataType.DataTypes.FIELD_LIST == updateMsg.payload().dataType())
                    decode(updateMsg.payload().fieldList());
                break;
        }
    }

    public void onStatusMsg(StatusMsg statusMsg, OmmConsumerEvent event)
    {
        switch(statusMsg.domainType()) {
            case EmaRdm.MMT_LOGIN://to handle Login Events which are similar to Connection Events
                System.out.println("Received LOGIN StatusMsg: ");
                System.out.println("State= " + statusMsg.state());
                break;
            case EmaRdm.MMT_DIRECTORY://to handle Source Directory Events which are similar to Market Data Service Events
                System.out.println("Received DIRECTORY StatusMsg: ");
                System.out.println(statusMsg);
                break;
            case EmaRdm.MMT_MARKET_PRICE://to handle Market Price Events which are similar to Market Data Subscription Events
                System.out.println("Received MARKET_PRICE StatusMsg:");
                System.out.println(statusMsg);
                break;
        }
    }

    public void onAckMsg(AckMsg ackMsg, OmmConsumerEvent event) {}
    public void onGenericMsg(GenericMsg genericMsg, OmmConsumerEvent event){}
    public void onAllMsg(Msg msg, OmmConsumerEvent event){}

    void decode( Msg msg )
    {
        switch(msg.attrib().dataType())
        {
            case DataTypes.ELEMENT_LIST:
                decode(msg.attrib().elementList());
                break;
        }

        switch(msg.payload().dataType())
        {
            case  DataTypes.ELEMENT_LIST:
                decode(msg.payload().elementList());
                break;
            case DataTypes.FIELD_LIST:
                decode(msg.payload().fieldList());
                break;
        }
    }

    void decode(ElementList elementList)
    {
        for(ElementEntry elementEntry : elementList)
        {
            System.out.print(" Name = " + elementEntry.name() + " DataType: " + DataType.asString(elementEntry.load().dataType()) + " Value: ");

            if (Data.DataCode.BLANK == elementEntry.code())
                System.out.println(" blank");
            else
                switch (elementEntry.loadType())
                {
                    case DataTypes.REAL :
                        System.out.println(elementEntry.real().asDouble());
                        break;
                    case DataTypes.DATE :
                        System.out.println(elementEntry.date().day() + " / " + elementEntry.date().month() + " / " + elementEntry.date().year());
                        break;
                    case DataTypes.TIME :
                        System.out.println(elementEntry.time().hour() + ":" + elementEntry.time().minute() + ":" + elementEntry.time().second() + ":" + elementEntry.time().millisecond());
                        break;
                    case DataTypes.INT :
                        System.out.println(elementEntry.intValue());
                        break;
                    case DataTypes.UINT :
                        System.out.println(elementEntry.uintValue());
                        break;
                    case DataTypes.ASCII :
                        System.out.println(elementEntry.ascii());
                        break;
                    case DataTypes.ENUM :
                        System.out.println(elementEntry.enumValue());
                        break;
                    case DataTypes.RMTES:
                        System.out.println(elementEntry.rmtes());
                        break;
                    case DataTypes.ERROR :
                        System.out.println(elementEntry.error().errorCode() +" (" + elementEntry.error().errorCodeAsString() + ")");
                        break;
                    case DataTypes.VECTOR :
                        decode(elementEntry.vector());
                        break;
                    default :
                        System.out.println();
                        break;
                }
        }
    }

    void decode(FieldList fieldList)
    {
        for (FieldEntry fieldEntry : fieldList)
        {
            System.out.print("Fid: " + fieldEntry.fieldId() + " Name = " + fieldEntry.name() + " DataType: " + DataType.asString(fieldEntry.load().dataType()) + " Value: ");

            if (Data.DataCode.BLANK == fieldEntry.code())
                System.out.println(" blank");
            else
                switch (fieldEntry.loadType())
                {
                    case DataTypes.REAL :
                        System.out.println(fieldEntry.real().asDouble());
                        break;
                    case DataTypes.DATE :
                        System.out.println(fieldEntry.date().day() + " / " + fieldEntry.date().month() + " / " + fieldEntry.date().year());
                        break;
                    case DataTypes.TIME :
                        System.out.println(fieldEntry.time().hour() + ":" + fieldEntry.time().minute() + ":" + fieldEntry.time().second() + ":" + fieldEntry.time().millisecond());
                        break;
                    case DataTypes.INT :
                        System.out.println(fieldEntry.intValue());
                        break;
                    case DataTypes.UINT :
                        System.out.println(fieldEntry.uintValue());
                        break;
                    case DataTypes.ASCII :
                        System.out.println(fieldEntry.ascii());
                        break;
                    case DataTypes.ENUM :
                        System.out.println(fieldEntry.hasEnumDisplay() ? fieldEntry.enumValue() + "(" + fieldEntry.enumDisplay() + ")":fieldEntry.enumValue());
                        break;
                    case DataTypes.RMTES:
                        System.out.println(fieldEntry.rmtes());
                        break;
                    case DataTypes.ERROR :
                        System.out.println(fieldEntry.error().errorCode() +" (" + fieldEntry.error().errorCodeAsString() + ")");
                        break;
                    default :
                        System.out.println();
                        break;
                }
        }
    }

    void decode(Vector vector)
    {
        switch (vector.summaryData().dataType())
        {
            case DataTypes.ELEMENT_LIST :
                decode(vector.summaryData().elementList());
                break;
        }

        for(VectorEntry vectorEntry : vector)
        {
            System.out.print("Position: " + vectorEntry.position() + " Action = " + vectorEntry.vectorActionAsString() + " DataType: " + DataType.asString(vectorEntry.loadType()) + " Value: ");

            switch (vectorEntry.loadType())
            {
                case DataTypes.ELEMENT_LIST :
                    decode(vectorEntry.elementList());
                    break;
                default:
                    System.out.println();
                    break;
            }
        }
    }
}

public class Consumer
{
    public static void main(String[] args)
    {
        OmmConsumer consumer = null;
        try
        {
            AppClient appClient = new AppClient();

            consumer  = EmaFactory.createOmmConsumer(EmaFactory.createOmmConsumerConfig().host("192.168.27.11:14002").username("pimchaya"), appClient);

            ReqMsg reqMsg = EmaFactory.createReqMsg();


            long directoryHandle = consumer.registerClient(reqMsg.domainType(EmaRdm.MMT_DIRECTORY).serviceName("ELEKTRON_DD"), appClient);
            
            long handle = consumer.registerClient(reqMsg.clear().serviceName("ELEKTRON_DD").name("EUR="), appClient);

            Thread.sleep(60000);			// API calls onRefreshMsg(), onUpdateMsg() and onStatusMsg()
        }
        catch (InterruptedException | OmmException excp)
        {
            System.out.println(excp.getMessage());
        }
        finally
        {
            if (consumer != null) consumer.uninitialize();
        }
    }
}
