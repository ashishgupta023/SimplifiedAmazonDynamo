package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Semaphore;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


public class SimpleDynamoProvider extends ContentProvider {
    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";


    static final int SERVER_PORT = 10000;
    String[] remotePorts = {REMOTE_PORT0, REMOTE_PORT1,REMOTE_PORT2 ,REMOTE_PORT3,REMOTE_PORT4};
    //NodeIdInDynamo , Port
    HashMap<String,String> activeAvds = new HashMap<String,String>();
    static String myPort;
    static String myPortForHash;
    private final Semaphore canDoWrite = new Semaphore(1, true);
    private final Semaphore canDoRead = new Semaphore(1, true);



    String replicaPort1;
    String replicaPort2;
    String predecessorPort;
    MatrixCursor globalCursor;
    String queryGlobalResult = null;
    boolean queryGlobalResponseReceived = false;
    HashMap<String,String> queryGlobalResultHashMap = new HashMap<String,String>();
    ArrayList<String> tempCursor = new ArrayList<String>();
    boolean failed = false;
    boolean insertFailed = false;

    String[] columnNames = {"key","value"};
    ArrayList<String> insertAck = new ArrayList<String>();

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.v("AVDINFO", "Creating Castle Black : My port is " + myPort);
        myPortForHash = portStr;
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }
        Log.v("AVDINFO" , "Add all nodes.");
        for (int i = 0 ; i < remotePorts.length ; i++) {
            try {
                activeAvds.put(genHash(Integer.toString(Integer.parseInt(remotePorts[i]) / 2)), remotePorts[i]);
            } catch (NoSuchAlgorithmException ex) {
                Log.e("SHA", "Error while creating SHA");
            }
        }


        String val = readFile("first");
        if(val!= null)
        {
            Log.v("flag" , "recovering node");
            getDataFromPredecessors();
            getDataFromSuccessors();
        }
        else
        {
            Log.v("flag","new node coming up " + myPort);
            Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
            String key = "first";
            String value = "first";
            ContentValues cv = new ContentValues();
            cv.put("key",key);
            cv.put("value", value);
            insertKeyValue(mUri, cv);
        }

        return false;
    }

    public void getDataFromPredecessors()
    {
        Log.v("deadComesAlive" , "Getting data from predecessors");

        //Organize all nodes in a ring
        List<String> nodeIDsinDynamo = new ArrayList<>(activeAvds.keySet());
        Collections.sort(nodeIDsinDynamo); // Sort by hashed port
        String tempPredecessorPort1 = null;
        String tempPredecessorPort2 = null;

        for (int i = 0; i < nodeIDsinDynamo.size(); i++) {
            if (activeAvds.get(nodeIDsinDynamo.get(i)).equals(myPort)) {
                if( i == 0) {
                    tempPredecessorPort1 = activeAvds.get(nodeIDsinDynamo.get(nodeIDsinDynamo.size() - 1));
                    tempPredecessorPort2 = activeAvds.get(nodeIDsinDynamo.get(nodeIDsinDynamo.size() - 2));
                }
                else if ( i == 1)
                {
                    tempPredecessorPort1 = activeAvds.get(nodeIDsinDynamo.get(0));
                    tempPredecessorPort2 = activeAvds.get(nodeIDsinDynamo.get(nodeIDsinDynamo.size() - 1));
                }
                else
                {
                    tempPredecessorPort1 = activeAvds.get(nodeIDsinDynamo.get(i-1));
                    tempPredecessorPort2 = activeAvds.get(nodeIDsinDynamo.get(i- 2));
                }
            }
        }
        Log.v("deadComesAlive" , "My Port " + myPort +  " : predecessor1 " + tempPredecessorPort1  +  " predecessor2 " + tempPredecessorPort2  );
        Log.v("deadComesAlive" , "Sending request to get what I should replicate. I am the slave of these masters.");
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, tempPredecessorPort1, "giveMeKeysToReplicate");
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, tempPredecessorPort2, "giveMeKeysToReplicate");
    }

    public void getDataFromSuccessors()
    {
        Log.v("deadComesAlive" , "Getting data from successors");

        //Organize all nodes in a ring
        List<String> nodeIDsinDynamo = new ArrayList<>(activeAvds.keySet());
        Collections.sort(nodeIDsinDynamo); // Sort by hashed port
        String tempSuccessorPort1 = null;
        String tempSuccessorPort2 = null;
        String myPredecessor = null;

        for (int i = 0; i < nodeIDsinDynamo.size(); i++) {
            if (activeAvds.get(nodeIDsinDynamo.get(i)).equals(myPort)) {
                tempSuccessorPort1 = activeAvds.get(nodeIDsinDynamo.get((i+1) % 5)) ;
                tempSuccessorPort2 = activeAvds.get(nodeIDsinDynamo.get((i+2) % 5)) ;
                if( i == 0) {
                    myPredecessor = activeAvds.get(nodeIDsinDynamo.get(nodeIDsinDynamo.size() - 1));
                }
                else
                {
                    myPredecessor = activeAvds.get(nodeIDsinDynamo.get(i - 1));

                }
            }
        }
        Log.v("deadComesAlive" , "My Port " + myPort +  " : predecessor1 " + tempSuccessorPort1  +  " predecessor2 " + tempSuccessorPort1  );
        Log.v("deadComesAlive" , "Sending request to slaves(successors who replicate my data) to claim what is mine because I am the one who knocks.");
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, tempSuccessorPort1, "giveMeMyKeys" , myPredecessor);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, tempSuccessorPort2, "giveMeMyKeys" , myPredecessor);
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        try {
            Log.v("insert" , "grabbing a write lock");
            canDoWrite.acquire();
        }
        catch(InterruptedException ex)
        {
            Log.v("lock" , "Error while grabbing a write lock");
        }
        synchronized (this) {
            String key = (String) values.get("key");
            String value = (String) values.get("value");
            Log.v("insert", "Received insert request for key " + key + " : " + value);

            //Organize all nodes in a ring
            List<String> nodeIDsinDynamo = new ArrayList<>(activeAvds.keySet());
            Collections.sort(nodeIDsinDynamo); // Sort by hashed port
            String tempPredecessorPort = null;
            for (int i = 0; i < nodeIDsinDynamo.size(); i++) {
                if (i == 0) {
                    tempPredecessorPort = activeAvds.get(nodeIDsinDynamo.get(nodeIDsinDynamo.size() - 1));
                } else {
                    tempPredecessorPort = activeAvds.get(nodeIDsinDynamo.get(i - 1));
                }

                String predecessorPortId = Integer.toString(Integer.parseInt(tempPredecessorPort) / 2);

                try {
                    if ((genHash(predecessorPortId).compareTo(genHash(key)) < 0 || genHash(key).compareTo(nodeIDsinDynamo.get(i)) <= 0) && i == 0) {
                        if (activeAvds.get(nodeIDsinDynamo.get(0)).equals(myPort)) {
                            Log.v("insert", " My Port " + myPort + " : " + key + " Belongs to me ");
                            //Inserting Locally
                            Log.v("insert", "Inserting the actual copy in " + myPort + "  : " + key + " -> " + value);
                            insertKeyValue(uri, values);
                            insertAck.add("ALIVE");
                            //Creating two replicas
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get(1)), "insertReplica", key, value);

                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get(2)), "insertReplica", key, value);
                            while( true )
                            {
                                // Waiting until replicas reply

                                if (insertAck.size() == 3)
                                    break;

                                if(insertFailed && insertAck.size()==2)
                                    break;
                            }

                            Log.v("insert" , "Received response from the writer quorum ");


                            insertAck.clear();
                            insertFailed = false;
                            break;
                        }
                        else {
                            //Redirecting to the correct node
                            Log.v("insert", " My Port " + myPort + " : " + key + " does not belong to me. Redirecting request to port " + activeAvds.get(nodeIDsinDynamo.get(i)));

                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get(i)), "insert", key, value);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get((i+1)%5)), "insertReplica", key, value);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get((i+2)%5)), "insertReplica", key, value);
                            while( true )
                            {
                                // Waiting until replicas reply

                                if (insertAck.size() == 3)
                                    break;

                                if(insertFailed && insertAck.size()==2)
                                    break;
                            }

                            Log.v("insert" , "Received response from the writer quorum ");


                            insertAck.clear();
                            insertFailed = false;
                            break;

                        }
                    } else if ((genHash(predecessorPortId).compareTo(genHash(key)) < 0 && genHash(key).compareTo(nodeIDsinDynamo.get(i)) <= 0) && i != 0) {

                        if (activeAvds.get(nodeIDsinDynamo.get(i)).equals(myPort)) {
                            Log.v("insert", " My Port " + myPort + " : " + key + "Belongs to me ");
                            //Inserting locally
                            Log.v("insert", "Inserting the actual copy in " + myPort + "  : " + key + " -> " + value);
                            insertAck.add("ALIVE");
                            insertKeyValue(uri, values);
                            //Creating two replicas

                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get((i + 1) % 5)), "insertReplica", key, value);

                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get((i + 2) % 5)), "insertReplica", key, value);

                            while( true )
                            {
                                // Waiting until replicas reply

                                if (insertAck.size() == 3)
                                    break;

                                if(insertFailed && insertAck.size()==2)
                                    break;
                            }

                            Log.v("insert" , "Received response from the writer quorum ");


                            insertAck.clear();
                            insertFailed = false;
                            break;

                        } else {
                            //Redirecting to the correct node
                            Log.v("insert", " My Port " + myPort + " : " + key + " does not belong to me. Redirecting request to port " + activeAvds.get(nodeIDsinDynamo.get(i)));

                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get(i)), "insert", key, value);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get((i+1)%5)), "insertReplica", key, value);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get((i+2)%5)), "insertReplica", key, value);
                            while( true )
                            {
                                // Waiting until replicas reply

                                if (insertAck.size() == 3)
                                    break;

                                if(insertFailed && insertAck.size()==2)
                                    break;
                            }

                            Log.v("insert" , "Received response from the writer quorum ");


                            insertAck.clear();
                            insertFailed = false;

                            break;
                        }
                    }
                } catch (NoSuchAlgorithmException ex) {
                    Log.e("SHA", "Error while creating SHA");

                }
            }
            Log.v("insert" , "releasing write lock");
            canDoWrite.release();

            return null;
        }


    }


    public Uri updateKeyValue(Uri uri, ContentValues values)
    {
        String key  = (String)values.get("key");
        String value = (String)values.get("value");
        String filename = key;
        FileOutputStream outputStream;

        try {
            String oldVal = readFile(key);
            if (oldVal == null)
            {
                Log.v("update", myPort + " inserting  - "+ key + " : " +  value );

                outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(value.getBytes());
                outputStream.close();
            }
            else
            {
                String[] oldValues = oldVal.split("~");
                String[] newValues = value.split("~");

                if( Integer.parseInt(newValues[1]) > Integer.parseInt(oldValues[1])  )
                {

                    Log.v("update", myPort + " updating  - "+ key + " : " +  value );

                    outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                    outputStream.write(value.getBytes());
                    outputStream.close();
                }

                if(Integer.parseInt(newValues[1]) == Integer.parseInt(oldValues[1]))
                {
                    if (Long.parseLong(newValues[2]) > Long.parseLong(oldValues[2]))
                    {
                        Log.v("update", myPort + " updating  - "+ key + " : " +  value );
                        outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                        outputStream.write(value.getBytes());
                        outputStream.close();
                    }
                }
            }

        } catch (Exception e)
        {
            Log.e("insert", "File write failed");
        }
        return uri;
    }

    public Uri insertKeyValue(Uri uri, ContentValues values)
    {
        String key  = (String)values.get("key");
        String value = (String)values.get("value");
        String filename = key;
        FileOutputStream outputStream;

        try {
            String oldVal = readFile(key);
            if (oldVal == null)
            {
                value = value + "~1~"+Long.toString(System.currentTimeMillis());
            }
            else
            {
                String[] oldValues = oldVal.split("~");
                value = value + "~" +  Integer.toString(Integer.parseInt(oldValues[1]) + 1 ) +"~"+ Long.toString(System.currentTimeMillis());
            }
            outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
            outputStream.write(value.getBytes());
            outputStream.close();
        } catch (Exception e)
        {
            Log.e("insert", "File write failed");
        }
        Log.v("insert", myPort + " Inserting  - "+ key + " : " +  value );
        return uri;
    }

    String getMostUpdatedResult(ArrayList<String> temp)
    {
        Log.v("query" ,"-----------" + temp.size());

        String qRes = temp.get(0);
        Log.v("query" ,"-----------" + qRes);
        if(temp.size() > 1) {

            for (int reader = 1; reader < temp.size(); reader++) {
                Log.v("query" ,"-----------" + temp.get(reader));
                if (temp.get(reader) != null && qRes!=null) {
                    if (Integer.parseInt(temp.get(reader).split("~")[1]) > Integer.parseInt(qRes.split("~")[1])) {
                        qRes = temp.get(reader);
                    }

                    if (Integer.parseInt(temp.get(reader).split("~")[1]) == Integer.parseInt(qRes.split("~")[1])) {
                        if (Long.parseLong(temp.get(reader).split("~")[2]) > Long.parseLong(qRes.split("~")[2])) {
                            qRes = temp.get(reader);

                        }
                    }
                }
                else if (qRes ==  null && temp.get(reader) != null)
                {
                    qRes = temp.get(reader);
                }
            }
        }
        return  qRes;
    }

    public void mergeGlobalResult(String tempRes) {
        String[] gResult = tempRes.split("-");


        for (int k = 0; k < gResult.length; k++) {

            if(queryGlobalResultHashMap.containsKey(gResult[k].split(":")[0])) {
                ArrayList<String> temp = new ArrayList<String>();
                temp.add(gResult[k].split(":")[1]);
                temp.add(queryGlobalResultHashMap.get(gResult[k].split(":")[0]));

                queryGlobalResultHashMap.put(gResult[k].split(":")[0], getMostUpdatedResult(temp));
            }
            else
            {
                queryGlobalResultHashMap.put(gResult[k].split(":")[0],gResult[k].split(":")[1] );

            }
            //globalCursor.addRow(new String[]{gResult[k].split(":")[0], gResult[k].split(":")[1]});
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        Log.v("query", " My Port " + myPort + " : " + selection + " query ");
        //Organize all nodes in a ring
        List<String> nodeIDsinDynamo = new ArrayList<>(activeAvds.keySet());
        Collections.sort(nodeIDsinDynamo); // Sort by hashed port
        String tempPredecessorPort = null ;

        if (selection.equals("\"@\""))
        {
           /* try {
                canDoRead.acquire();
            }
            catch(InterruptedException ex)
            {
                Log.v("lock" , "Error while grabbing a read lock");
            }*/

            synchronized (this) {
                MatrixCursor cursor = new MatrixCursor(columnNames, 0);

                Log.v("query", " Retrieving local key:value pairs");
                File dir = new File(System.getProperty("user.dir") + "data/data/edu.buffalo.cse.cse486586.simpledynamo/files");
                File[] files = dir.listFiles();
                if (files != null) {
                    for (int i = 0; i < files.length; i++) {
                        if(!files[i].getName().equals("first")) {
                            cursor.addRow(new String[]{files[i].getName(), readFile(files[i].getName()).split("~")[0]});
                        }
                    }
                }
                while (cursor.moveToNext()) {
                    int keyIndex = cursor.getColumnIndex("key");
                    int valueIndex = cursor.getColumnIndex("value");
                    String key = cursor.getString(keyIndex);
                    String value = cursor.getString(valueIndex);
                    Log.v("query", "< " + key + " : " + value + " > ");
                }
                //canDoRead.release();
                return cursor;
            }
        }
        if (selection.equals("\"@+\""))
        {

                MatrixCursor cursor = new MatrixCursor(columnNames, 0);

                Log.v("query", " Retrieving local key:value pairs");
                File dir = new File(System.getProperty("user.dir") + "data/data/edu.buffalo.cse.cse486586.simpledynamo/files");
                File[] files = dir.listFiles();
                if (files != null) {
                    for (int i = 0; i < files.length; i++) {
                        if(!files[i].getName().equals("first")) {
                            cursor.addRow(new String[]{files[i].getName(), readFile(files[i].getName())});
                        }
                    }
                }
                while (cursor.moveToNext()) {
                    int keyIndex = cursor.getColumnIndex("key");
                    int valueIndex = cursor.getColumnIndex("value");
                    String key = cursor.getString(keyIndex);
                    String value = cursor.getString(valueIndex);
                    Log.v("query", "< " + key + " : " + value + " > ");
                }

                return cursor;

        }
        else if (selection.equals("\"*\""))
        {
            globalCursor = new MatrixCursor(columnNames,0);
            Log.v("queryglobal", " Retrieving everything this dynamo world has in key:value pairs");
            File dir = new File(System.getProperty("user.dir") + "data/data/edu.buffalo.cse.cse486586.simpledynamo/files");
            File[] files = dir.listFiles();

            if (files != null) {
                Log.v("queryglobal" ,"Files Found");
                for (int i = 0; i < files.length; i++) {
                    if(!files[i].getName().equals("first")) {
                        queryGlobalResultHashMap.put(files[i].getName() ,readFile(files[i].getName()) );
                    }
                }
            }

            for (int i = 0; i < nodeIDsinDynamo.size(); i++)
            {
                if(!myPort.equals(activeAvds.get(nodeIDsinDynamo.get(i))))
                {
                    Log.v("queryglobal" , "I am not the only one who knocks");

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get(i)), "queryGlobal");

                    while (!queryGlobalResponseReceived) {
                    }
                    Log.v("queryglobal", "Query Global Responses received " + queryGlobalResult);
                    queryGlobalResponseReceived = false;
                    if(queryGlobalResult!=null ) {
                        if (queryGlobalResult.length() != 0) {

                            mergeGlobalResult(queryGlobalResult);

                        }
                    }
                    queryGlobalResult = null;
                }
                Log.v("queryglobal", "Query Global Responses Current Hash Map Size" + queryGlobalResultHashMap.size());

            }
            for(String key : queryGlobalResultHashMap.keySet())
            {
                globalCursor.addRow(new String[]{key, queryGlobalResultHashMap.get(key).split("~")[0]});
            }
            queryGlobalResultHashMap.clear();

            globalCursor.moveToPosition(-1);
            while (globalCursor.moveToNext()) {
                int keyIndex = globalCursor.getColumnIndex("key");
                int valueIndex = globalCursor.getColumnIndex("value");
                String key = globalCursor.getString(keyIndex);
                String value = globalCursor.getString(valueIndex);
                Log.v("queryglobal", "< " + key + " : " + value + " > ");
            }

            return  globalCursor;
        }
        else
        {
            try {
                Log.v("query" , "grabbing a read lock");
                canDoRead.acquire();
            }
            catch(InterruptedException ex)
            {
                Log.v("lock" , "Error while grabbing a  read lock");
            }

            synchronized (this) {
                MatrixCursor cursor = new MatrixCursor(columnNames, 0);
                for (int i = 0; i < nodeIDsinDynamo.size(); i++) {
                    if (i == 0) {
                        tempPredecessorPort = activeAvds.get(nodeIDsinDynamo.get(nodeIDsinDynamo.size() - 1));
                    } else {
                        tempPredecessorPort = activeAvds.get(nodeIDsinDynamo.get(i - 1));
                    }

                    String predecessorPortId = Integer.toString(Integer.parseInt(tempPredecessorPort) / 2);

                    try {

                        if ((genHash(predecessorPortId).compareTo(genHash(selection)) < 0 || genHash(selection).compareTo(nodeIDsinDynamo.get(i)) <= 0) && i == 0) {
                            if (activeAvds.get(nodeIDsinDynamo.get(0)).equals(myPort)) {
                                Log.v("query", " My Port " + myPort + " : " + selection + " Belongs to me (first) ");
                                //query locally

                                String val = readFile(selection);
                                if (val != null) {

                                    tempCursor.add(val);
                                }
                                //Waiting for readers in reader quorum

                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get((i+1)%5)), "query", selection);
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get((i+2)%5)), "query", selection);

                                while( true )
                                {
                                    // Waiting until replicas reply

                                    if (tempCursor.size() == 3)
                                        break;

                                    if(failed && tempCursor.size()==2)
                                        break;
                                }

                                Log.v("query" , "Received response from the reader quorum ");

                                String res = getMostUpdatedResult(tempCursor);

                                tempCursor.clear();
                                failed = false;
                                cursor.addRow(new String[]{selection, res.split("~")[0]});
                                Log.v("query", selection + ":" + cursor.getCount());


                                while (cursor.moveToNext()) {
                                    int keyIndex = cursor.getColumnIndex("key");
                                    int valueIndex = cursor.getColumnIndex("value");
                                    String key = cursor.getString(keyIndex);
                                    String value = cursor.getString(valueIndex);
                                    Log.v("query", "< " + key + " : " + value + " > ");
                                }
                                Log.v("query" , "Releasing read lock");
                                canDoRead.release();
                                return cursor;
                            } else {
                                //Redirecting to the correct node
                                Log.v("query", " My Port " + myPort + " : " + selection + " does not belong to me . Redirecting request to (first) " + activeAvds.get(nodeIDsinDynamo.get(i)));

                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get(i)), "query", selection);
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get((i+1)%5)), "query", selection);
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get((i+2)%5)), "query", selection);
                                while( true )
                                {
                                    // Waiting until replicas reply

                                    if (tempCursor.size() == 3)
                                        break;

                                    if(failed && tempCursor.size()==2)
                                        break;
                                }
                                Log.v("query" , "Received response from the reader quorum ");

                                String res = getMostUpdatedResult(tempCursor);

                                tempCursor.clear();
                                failed = false;
                                cursor.addRow(new String[]{selection, res.split("~")[0]});
                                Log.v("query", selection + ":" + cursor.getCount());


                                while (cursor.moveToNext()) {
                                    int keyIndex = cursor.getColumnIndex("key");
                                    int valueIndex = cursor.getColumnIndex("value");
                                    String key = cursor.getString(keyIndex);
                                    String value = cursor.getString(valueIndex);
                                    Log.v("query", "< " + key + " : " + value + " > ");
                                }
                                Log.v("query" , "Releasing read lock");

                                canDoRead.release();
                                return cursor;

                            }
                        } else if ((genHash(predecessorPortId).compareTo(genHash(selection)) < 0 && genHash(selection).compareTo(nodeIDsinDynamo.get(i)) <= 0) && i != 0) {

                            if (activeAvds.get(nodeIDsinDynamo.get(i)).equals(myPort)) {
                                Log.v("query", " My Port " + myPort + " : " + selection + " Belongs to me ");
                                //query locally

                                String val = readFile(selection);
                                if (val != null) {

                                    tempCursor.add(val);
                                }
                                //Waiting for readers in reader quorum

                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get((i+1)%5)), "query", selection);
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get((i+2)%5)), "query", selection);

                                while( true )
                                {
                                    // Waiting until replicas reply

                                    if (tempCursor.size() == 3)
                                        break;

                                    if(failed && tempCursor.size()==2)
                                        break;
                                }

                                Log.v("query" , "Received response from the reader quorum ");

                                String res = getMostUpdatedResult(tempCursor);

                                tempCursor.clear();
                                failed = false;
                                cursor.addRow(new String[]{selection, res.split("~")[0]});
                                Log.v("query", selection + ":" + cursor.getCount());

                                while (cursor.moveToNext()) {
                                    int keyIndex = cursor.getColumnIndex("key");
                                    int valueIndex = cursor.getColumnIndex("value");
                                    String key = cursor.getString(keyIndex);
                                    String value = cursor.getString(valueIndex);
                                    Log.v("query", "< " + key + " : " + value + " > ");
                                }
                                Log.v("query" , "Releasing read lock");

                                canDoRead.release();

                                return cursor;
                            } else {
                                //Redirecting to the correct node
                                Log.v("query", " My Port " + myPort + " : " + selection + " does not belong to me . Redirecting request to " + activeAvds.get(nodeIDsinDynamo.get(i)));

                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get(i)), "query", selection);
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get((i+1)%5)), "query", selection);
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get((i + 2) % 5)), "query", selection);
                                while( true )
                                {
                                    // Waiting until replicas reply

                                    if (tempCursor.size() == 3)
                                        break;

                                    if(failed && tempCursor.size()==2)
                                        break;
                                }

                                Log.v("query" , "Received response from the reader quorum ");

                                String res = getMostUpdatedResult(tempCursor);

                                tempCursor.clear();
                                failed = false;
                                cursor.addRow(new String[]{selection, res.split("~")[0]});
                                Log.v("query", selection + ":" + cursor.getCount());


                                while (cursor.moveToNext()) {
                                    int keyIndex = cursor.getColumnIndex("key");
                                    int valueIndex = cursor.getColumnIndex("value");
                                    String key = cursor.getString(keyIndex);
                                    String value = cursor.getString(valueIndex);
                                    Log.v("query", "< " + key + " : " + value + " > ");
                                }
                                Log.v("query" , "Releasing read lock");

                                canDoRead.release();
                                return cursor;

                            }
                        }

                    } catch (NoSuchAlgorithmException ex) {
                        Log.e("SHA", "Error while creating SHA");

                    }

                }
            }
        }

        return null;
    }

    public String readFile(String key)
    {
        try {
            FileInputStream fis;
            String value = null;
            fis = getContext().openFileInput(key);
            if (fis != null) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fis));
                String val;
                while ((val = br.readLine()) != null) {
                    if (value == null)
                        value = val;
                    else
                        value = value + val;
                }
            }

            return  value;
        }
        catch(Exception ex)
        {
            Log.e("query", "Error reading file with key " + key);
            return null;
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        Log.v("delete", " Valar Morghulis: My Port " + myPort + " : " + selection + " query ");
      /*  try {
            canDoWrite.acquire();
        }
        catch(InterruptedException ex)
        {
            Log.v("lock" , "Error while grabbing a write lock");
        }*/
        synchronized (this) {
            String tempPredecessorPort = null;
            //Organize all nodes in a ring
            List<String> nodeIDsinDynamo = new ArrayList<>(activeAvds.keySet());
            Collections.sort(nodeIDsinDynamo); // Sort by hashed port

            for (int i = 0; i < nodeIDsinDynamo.size(); i++) {
                if (i == 0) {
                    tempPredecessorPort = activeAvds.get(nodeIDsinDynamo.get(nodeIDsinDynamo.size() - 1));
                } else {
                    tempPredecessorPort = activeAvds.get(nodeIDsinDynamo.get(i - 1));
                }

                String predecessorPortId = Integer.toString(Integer.parseInt(tempPredecessorPort) / 2);

                try {

                    if ((genHash(predecessorPortId).compareTo(genHash(selection)) < 0 || genHash(selection).compareTo(nodeIDsinDynamo.get(i)) <= 0) && i == 0) {
                        if (activeAvds.get(nodeIDsinDynamo.get(0)).equals(myPort)) {
                            Log.v("delete", "Valar Morghulis: My Port " + myPort + " : " + selection + " Belongs to me ");
                            //delete locally

                            deleteFile(selection);

                            //Delete in two replicas
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get(1)), "deleteReplica", selection);

                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get(2)), "deleteReplica", selection);

                            break;
                            //Waiting for delete in writers quorum
                        } else {
                            //Redirecting to the correct node
                            Log.v("delete", "Valar Morghulis: My Port " + myPort + " : " + selection + " does not belong to me . Redirecting request to " + activeAvds.get(nodeIDsinDynamo.get(i)));

                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get(i)), "delete", selection);
                            break;
                        }
                    } else if ((genHash(predecessorPortId).compareTo(genHash(selection)) < 0 && genHash(selection).compareTo(nodeIDsinDynamo.get(i)) <= 0) && i != 0) {

                        if (activeAvds.get(nodeIDsinDynamo.get(i)).equals(myPort)) {
                            Log.v("delete", " My Port " + myPort + " : " + selection + " Belongs to me ");
                            //delete locally

                            deleteFile(selection);

                            //Delete in two replicas
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get((i + 1) % 5)), "deleteReplica", selection);

                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get((i + 2) % 5)), "deleteReplica", selection);

                            break;
                            //Waiting for delete in writers quorum

                        } else {
                            //Redirecting to the correct node
                            Log.v("delete", " My Port " + myPort + " : " + selection + " does not belong to me . Redirecting request to " + activeAvds.get(nodeIDsinDynamo.get(i)));

                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get(i)), "delete", selection);
                            break;
                        }
                    }

                } catch (NoSuchAlgorithmException ex) {
                    Log.e("SHA", "Error while creating SHA");

                }

            }
            //canDoWrite.release();
            return 0;
        }
    }

    public void deleteFile(String key)
    {
        FileInputStream fis;
        Log.v("delete", " Deleting key:value pair");
        File dir = new File(System.getProperty("user.dir") + "data/data/edu.buffalo.cse.cse486586.simpledynamo/files");
        File[] files = dir.listFiles();

        if (files != null) {
            for (int p = 0; p < files.length; p++) {
                if (files[p].getName().equals(key)) {
                    files[p].delete();
                }
            }
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    public MessageObject processMessage(String message) {
        String[] moInStr = message.split("#");
        MessageObject mo = new MessageObject();
        mo.setSender(moInStr[0]);
        mo.setReceiver(moInStr[1]);
        mo.setOperation(moInStr[2]);

        if(moInStr.length > 3 && mo.getOperation().equals("insert"))
        {
            mo.setKey(moInStr[3]);
            mo.setValue(moInStr[4]);
        }
        if(moInStr.length > 3 && mo.getOperation().equals("insertReplica"))
        {
            mo.setKey(moInStr[3]);
            mo.setValue(moInStr[4]);
        }
        if(moInStr.length > 3 && mo.getOperation().equals("query"))
        {
            mo.setKey(moInStr[3]);
        }


        if(moInStr.length > 3 && mo.getOperation().equals("delete"))
        {
            mo.setKey(moInStr[3]);
        }
        if(moInStr.length > 3 && mo.getOperation().equals("deleteReplica"))
        {
            mo.setKey(moInStr[3]);
        }
        if(mo.getOperation().equals("keysToInsert"))
        {
            mo.setValue(moInStr[3]);
        }
        if(mo.getOperation().equals("giveMeMyKeys"))
        {
            mo.setKey(moInStr[3]);
        }

        return mo;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            while (true) {

                ServerSocket serverSocket = sockets[0];
                try {
                    Socket clientSocket = serverSocket.accept();
                    ObjectInputStream ois = new ObjectInputStream(clientSocket.getInputStream());
                    String message =(String)ois.readObject();

                    MessageObject action = processMessage(message);
                    Log.v("server" , "Received from " + action.getSender());
                    if(action.getOperation().equals("insert"))
                    {
                        Log.v("insert","Inserting at " + myPort + " :  " + action.getKey() + "->" + action.getValue());
                        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                        String key = action.getKey();
                        String value = action.getValue();
                        ContentValues cv = new ContentValues();
                        cv.put("key",key);
                        cv.put("value", value);
                        //Inserting Locally
                        insertKeyValue(mUri, cv);
                        //Creating two replicas

                       /* //Organize all nodes in a ring
                        List<String> nodeIDsinDynamo = new ArrayList<>(activeAvds.keySet());
                        Collections.sort(nodeIDsinDynamo); // Sort by hashed port

                        for(int i = 0; i < nodeIDsinDynamo.size() ; i++) {
                            if(activeAvds.get(nodeIDsinDynamo.get(i)).equals(myPort)) {
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get((i+1) % 5)), "insertReplica", key, value);

                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get((i+2) % 5)), "insertReplica", key, value);
                            }
                        }*/

                        Log.v("insert" ,"sending alive notice");
                        ObjectOutputStream oos = new ObjectOutputStream(clientSocket.getOutputStream());
                        oos.writeObject("ALIVE");
                        oos.close();
                        ois.close();

                    }
                    else if(action.getOperation().equals("insertReplica"))
                    {
                        Log.v("insert","Request from " +   action.getSender() + " :Inserting replica on "  + myPort  + " for " + action.getKey() + " -> " + action.getValue());
                        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                        String key = action.getKey();
                        String value = action.getValue();
                        ContentValues cv = new ContentValues();
                        cv.put("key",key);
                        cv.put("value", value);
                        //Inserting Replica
                        insertKeyValue(mUri, cv);
                        Log.v("insert" ,"sending alive notice from replica");
                        ObjectOutputStream oos = new ObjectOutputStream(clientSocket.getOutputStream());
                        oos.writeObject("ALIVE");
                        oos.close();
                        ois.close();
                    }
                    else if( action.getOperation().equals("query"))
                    {
                        synchronized (this) {
                            Log.v("query", "Received query  request from " + action.getSender() + " for " + action.getKey());

                            ObjectOutputStream oos = new ObjectOutputStream(clientSocket.getOutputStream());
                            Log.v("query", "Sending query  result as " + readFile(action.getKey()));
                            oos.writeObject(readFile(action.getKey()));

                            oos.close();
                            ois.close();
                        }

                    }
                    else if(action.getOperation().equals("queryGlobal"))
                    {

                        Log.v("queryglobal", "Received queryglobal request from " + action.getSender());

                        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

                        Cursor resultCursor = query(mUri, null, "\"@+\"",
                                null, null);
                        String result = null;
                        Log.v("queryglobal", "Checking " + resultCursor.getCount());
                        resultCursor.moveToPosition(-1);
                        while (resultCursor.moveToNext()) {
                            int keyIndex = resultCursor.getColumnIndex("key");
                            int valueIndex = resultCursor.getColumnIndex("value");
                            String key = resultCursor.getString(keyIndex);
                            String value = resultCursor.getString(valueIndex);
                            Log.v("queryglobal", "Checking " + key + ":" + value);


                            if (result == null)
                                result = key + ":" + value;
                            else
                                result += "-" + key + ":" + value;

                        }
                        if (result == null )
                            result = "";

                        Log.v("queryglobal", result);
                        ObjectOutputStream oos = new ObjectOutputStream(clientSocket.getOutputStream());
                        Log.v("query", "Sending query result as " + result);

                        oos.writeObject(result);
                        oos.close();
                        ois.close();

                    }
                    else if(action.getOperation().equals("delete"))
                    {
                        synchronized (this) {
                            Log.v("delete", "Valar Morghulis: Received delete request from " + action.getSender());
                            Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

                            deleteFile(action.getKey());

                            //Delete two replicas

                            //Organize all nodes in a ring
                            List<String> nodeIDsinDynamo = new ArrayList<>(activeAvds.keySet());
                            Collections.sort(nodeIDsinDynamo); // Sort by hashed port

                            for(int i = 0; i < nodeIDsinDynamo.size() ; i++) {
                                if(activeAvds.get(nodeIDsinDynamo.get(i)).equals(myPort)) {
                                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get((i+1) % 5)), "deleteReplica",action.getKey() );

                                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, activeAvds.get(nodeIDsinDynamo.get((i+2) % 5)), "deleteReplica", action.getKey());
                                }
                            }
                            Log.v("delete", "Sending a alive notice");
                            ObjectOutputStream oos = new ObjectOutputStream(clientSocket.getOutputStream());
                            oos.writeObject("ALIVE");
                            oos.close();
                            ois.close();

                        }

                    }
                    else if(action.getOperation().equals("deleteReplica"))
                    {
                        synchronized (this) {
                            Log.v("delete", "Valar Morghulis: Received delete Replica request from " + action.getSender());
                            Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

                            deleteFile(action.getKey());


                        }

                    }
                    else if(action.getOperation().equals("giveMeKeysToReplicate"))
                    {
                        Log.v("deadComesAlive", "Received a request from " + action.getSender() + " to give it keys to replicate");
                        //Organize all nodes in a ring
                        List<String> nodeIDsinDynamo = new ArrayList<>(activeAvds.keySet());
                        Collections.sort(nodeIDsinDynamo); // Sort by hashed port
                        String myPredecessor = null;
                        boolean firstInRing = false;
                        for (int i = 0; i < nodeIDsinDynamo.size(); i++) {
                            if (activeAvds.get(nodeIDsinDynamo.get(i)).equals(myPort)) {

                                if (i == 0) {
                                    firstInRing = true;
                                    myPredecessor = activeAvds.get(nodeIDsinDynamo.get(nodeIDsinDynamo.size() - 1));
                                } else {
                                    myPredecessor = activeAvds.get(nodeIDsinDynamo.get(i - 1));

                                }
                            }
                        }
                        String myPredecessorPortId = Integer.toString(Integer.parseInt(myPredecessor)/2);
                        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

                        Cursor resultCursor = query(mUri, null, "\"@+\"",
                                null, null);
                        String result = null;
                        Log.v("deadComesAlive", "Checking " + resultCursor.getCount());
                        resultCursor.moveToPosition(-1);
                        while (resultCursor.moveToNext()) {
                            int keyIndex = resultCursor.getColumnIndex("key");
                            int valueIndex = resultCursor.getColumnIndex("value");
                            String key = resultCursor.getString(keyIndex);
                            String value = resultCursor.getString(valueIndex);
                            Log.v("deadComesAlive", "Checking " + key + ":" + value);
                            try {
                                if (firstInRing) {
                                    if ((genHash(myPredecessorPortId).compareTo(genHash(key)) < 0) || (genHash(key).compareTo(genHash(myPortForHash)) <= 0)) {
                                        if (result == null)
                                            result = key + ":" + value;
                                        else
                                            result += "-" + key + ":" + value;
                                    }
                                }
                                else
                                {
                                    if ((genHash(myPredecessorPortId).compareTo(genHash(key)) < 0) && (genHash(key).compareTo(genHash(myPortForHash)) <= 0)) {
                                        if (result == null)
                                            result = key + ":" + value;
                                        else
                                            result += "-" + key + ":" + value;
                                    }
                                }
                            }
                            catch (NoSuchAlgorithmException ex)
                            {
                                Log.v("deadComesAlive" , "Problem while getting SHA");
                            }
                        }
                        if (result == null )
                            result = "";
                        Log.v("deadComesAlive" , "Sending keys to recovering node " + action.getSender() + " to replicate: " + result);

                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, action.getSender(), "keysToInsert", result);
                    }
                    else if(action.getOperation().equals("giveMeMyKeys"))
                    {
                        Log.v("deadComesAlive", "Received a request from " + action.getSender() + " to give it keys which belong to it ");
                        //Organize all nodes in a ring
                        List<String> nodeIDsinDynamo = new ArrayList<>(activeAvds.keySet());
                        Collections.sort(nodeIDsinDynamo); // Sort by hashed port
                        String sendersPredecessor = action.getKey();
                        String sender = action.getSender();

                        boolean senderFirstInRing = false;
                        for (int i = 0; i < nodeIDsinDynamo.size(); i++) {
                            if (activeAvds.get(nodeIDsinDynamo.get(i)).equals(sender)) {
                                if (i == 0) {
                                    senderFirstInRing = true;
                                }
                            }
                        }
                        String sendersPredecessorPortId = Integer.toString(Integer.parseInt(sendersPredecessor)/2);
                        String sendersPortId = Integer.toString(Integer.parseInt(sender)/2);
                        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

                        Cursor resultCursor = query(mUri, null, "\"@+\"",
                                null, null);
                        String result = null;
                        Log.v("deadComesAlive", "Checking " + resultCursor.getCount());
                        resultCursor.moveToPosition(-1);
                        while (resultCursor.moveToNext()) {
                            int keyIndex = resultCursor.getColumnIndex("key");
                            int valueIndex = resultCursor.getColumnIndex("value");
                            String key = resultCursor.getString(keyIndex);
                            String value = resultCursor.getString(valueIndex);
                            Log.v("deadComesAlive", "Checking " + key + ":" + value);
                            try {
                                if (senderFirstInRing) {
                                    if ((genHash(sendersPredecessorPortId).compareTo(genHash(key)) < 0) || (genHash(key).compareTo(genHash(sendersPortId)) <= 0)) {
                                        if (result == null)
                                            result = key + ":" + value;
                                        else
                                            result += "-" + key + ":" + value;
                                    }
                                }
                                else
                                {
                                    if ((genHash(sendersPredecessorPortId).compareTo(genHash(key)) < 0) && (genHash(key).compareTo(genHash(sendersPortId)) <= 0)) {
                                        if (result == null)
                                            result = key + ":" + value;
                                        else
                                            result += "-" + key + ":" + value;
                                    }
                                }
                            }
                            catch (NoSuchAlgorithmException ex)
                            {
                                Log.v("deadComesAlive" , "Problem while getting SHA");
                            }
                        }
                        if (result == null )
                            result = "";
                        Log.v("deadComesAlive" , "Sending keys to recovering node " + action.getSender() + " for which it is the co-ordinator: " + result);

                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, action.getSender(), "keysToInsert", result);
                    }
                    else if (action.getOperation().equals("keysToInsert"))
                    {

                        Log.v("deadComesAlive" , "Got the keys to insert from " + action.getSender() + " as " + action.getValue()) ;
                        String toReplicate = action.getValue();
                        if(!toReplicate.equals("null") ) {
                            if (toReplicate.length() != 0) {
                                String[] gResult = toReplicate.split("-");
                                for (int k = 0; k < gResult.length; k++) {

                                    Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                                    String key = gResult[k].split(":")[0];
                                    String value = gResult[k].split(":")[1];
                                    ContentValues cv = new ContentValues();
                                    cv.put("key",key);
                                    cv.put("value", value);
                                    //Got keys to replicate
                                    updateKeyValue(mUri, cv);
                                }
                            }
                        }
                    }

                }
                catch (IOException ex)
                {
                    ex.printStackTrace();
                    Log.e(TAG, "IO Exception while receiving message " + ex.getMessage() );
                }
                catch (Exception ex)
                {
                    ex.printStackTrace();
                    Log.e(TAG, "Exception while receiving message " + ex.getMessage()  );
                }
            }
        }
    }




    /*public void insertIntoReplicas(String coOrdinator,String key, String value)
    {
        List<String> nodeIDsinDynamo = new ArrayList<>(activeAvds.keySet());
        Collections.sort(nodeIDsinDynamo); // Sort by hashed port
        String tempSuccessorPort1 = null;
        String tempSuccessorPort2 = null;


        for (int i = 0; i < nodeIDsinDynamo.size(); i++) {
            if (activeAvds.get(nodeIDsinDynamo.get(i)).equals(coOrdinator)) {
                tempSuccessorPort1 = activeAvds.get(nodeIDsinDynamo.get((i+1) % 5)) ;
                tempSuccessorPort2 = activeAvds.get(nodeIDsinDynamo.get((i+2) % 5)) ;
            }
        }
        //Contacting replicas
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, tempSuccessorPort1 , "insertReplica", key, value);

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, tempSuccessorPort2 , "insertReplica", key, value);

       *//* if(canDoWrite.availablePermits() == 0)
        {
            canDoWrite.release();
        }*//*
    }*/

    public void deleteFromReplicas(String coOrdinator,String key)
    {
        List<String> nodeIDsinDynamo = new ArrayList<>(activeAvds.keySet());
        Collections.sort(nodeIDsinDynamo); // Sort by hashed port
        String tempSuccessorPort1 = null;
        String tempSuccessorPort2 = null;


        for (int i = 0; i < nodeIDsinDynamo.size(); i++) {
            if (activeAvds.get(nodeIDsinDynamo.get(i)).equals(coOrdinator)) {
                tempSuccessorPort1 = activeAvds.get(nodeIDsinDynamo.get((i+1) % 5)) ;
                tempSuccessorPort2 = activeAvds.get(nodeIDsinDynamo.get((i+2) % 5)) ;
            }
        }
        //Contacting replicas
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, tempSuccessorPort1 , "deleteReplica", key);

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, tempSuccessorPort2 , "deleteReplica", key);

        /*if(canDoWrite.availablePermits() == 0)
        {
            canDoWrite.release();
        }*/
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            if ((msgs[2].equals("insert") || msgs[2].equals("insertReplica")) && msgs.length == 5) {
                try {

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));
                    //if (msgs[2].equals("insert")) {
                        socket.setSoTimeout(2000);
                    //}
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    //sender#receiver#operation#key#value
                    oos.writeObject(msgs[0] + "#" + msgs[1] + "#"+ msgs[2]+ "#" + msgs[3] + "#" + msgs[4]);
                           Log.v("insert", "Sending a " + msgs[2] + " message to " + msgs[1] + " from " + msgs[0]);
                    //if(msgs[2].equals("insert")) {
                        ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

                        Log.v("insert", "Waiting for ping ack here................");
                        String message = (String) ois.readObject();
                        Log.v("insert", "Got response from " + msgs[1] + " as " + message);
                        if (canDoWrite.availablePermits() == 0)
                            insertAck.add(message);
                        ois.close();
                    //}
                    oos.close();
                    socket.close();

                } catch (UnknownHostException e) {

                    Log.e(TAG, "ClientTask UnknownHostException");
                }
                catch(SocketTimeoutException ex)
                {
                    Log.e(TAG, "ClientTask SocketTimeOut from " + msgs[1] + " " + msgs[2]);
                    Log.v("insert", "Someone died");
                    if (canDoWrite.availablePermits() == 0)
                        insertFailed = true;

                   /* if (msgs[2].equals("insert"))
                    {
                        Log.e(TAG,"Coordintaor seems to be dead. Contacting replicas");

                        insertIntoReplicas(msgs[1], msgs[3], msgs[4]);
                    }*/

                }
                catch (IOException e) {

                    Log.e(TAG, "ClientTask socket IOException from " + msgs[1] );
                    Log.v("insert", "Someone died");
                    if (canDoWrite.availablePermits() == 0)
                        insertFailed = true;

                    //Log.e(TAG, e.getMessage());

                   /* if (msgs[2].equals("insert"))
                    {
                        Log.v("insert" , "Coordinator seems to be dead . Contacting replicas" );

                        insertIntoReplicas(msgs[1], msgs[3], msgs[4]);
                    }*/

                }
                catch( Exception ex)
                {
                    Log.e(TAG, "ClientTask socket Exception");

                }
            }
            else if(msgs[2].equals("query")  && msgs.length == 4)
            {
                synchronized (this) {
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(msgs[1]));
                        socket.setSoTimeout(2000);
                        //sender#receiver#operation#key
                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                        oos.writeObject(msgs[0] + "#" + msgs[1] + "#query#" + msgs[3]);

                        Log.v("query", "Sending a query message to " + msgs[1] + " from " + msgs[0]);

                        ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

                        Log.v("insert", "Waiting for reply here................");
                        String message = (String) ois.readObject();
                        Log.v("insert", "Reply received as " + message);
                        if(canDoRead.availablePermits() == 0)
                            tempCursor.add(message);
                        ois.close();
                        oos.close();
                        socket.close();


                    } catch (UnknownHostException e) {

                        Log.e(TAG, "ClientTask UnknownHostException");
                    } catch (SocketTimeoutException ex) {
                        Log.e(TAG, "ClientTask SocketTimeOut from " + msgs[1] + " " + msgs[2] + " key " + msgs[3]);
                        Log.v("query", "Someone died");
                       if(canDoRead.availablePermits() == 0)
                            failed = true;


                    } catch (IOException e) {

                        Log.e(TAG, "ClientTask socket IOException from " + msgs[1] );
                        //Log.e(TAG, e.getMessage());
                        Log.v("query", "Someone died");
                        if(canDoRead.availablePermits() == 0)
                            failed = true;

                    } catch (Exception ex) {
                        Log.e(TAG, "ClientTask socket Exception");
                        Log.e(TAG, ex.getMessage());
                    }
                }
            }

            else if(msgs[2].equals("queryGlobal"))
            {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));
                    socket.setSoTimeout(2000);
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    //sender#receiver#operation
                    oos.writeObject(msgs[0]+"#"+msgs[1]+"#queryGlobal");
                    Log.v("query", "Sending a queryGlobal request to " + msgs[1] + " from " + msgs[0]);

                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

                    Log.v("insert", "Waiting for reply here................");
                    String message = (String) ois.readObject();
                    Log.v("insert", "Reply received as "+ message);
                    queryGlobalResult = message;
                    queryGlobalResponseReceived = true;
                    ois.close();
                    oos.close();
                    socket.close();

                } catch (UnknownHostException e) {

                    Log.e(TAG, "ClientTask UnknownHostException");
                }
                catch (SocketTimeoutException ex)
                {
                    Log.e(TAG, "ClientTask SocketTimeOut from " + msgs[1] + " " + msgs[2]);

                    queryGlobalResult = null;
                    queryGlobalResponseReceived = true;
                }
                catch (IOException e) {

                    Log.e(TAG, "ClientTask socket IOException");
                    //Log.e(TAG, e.getMessage());

                    queryGlobalResult = null;
                    queryGlobalResponseReceived = true;
                }
                catch(Exception ex)
                {
                    Log.e(TAG, "ClientTask socket Exception");
                    Log.e(TAG, ex.getMessage());
                }
            }
            else if(msgs[2].equals("delete"))
            {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));
                    socket.setSoTimeout(2000);
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    //sender#receiver#operation#key
                    oos.writeObject(msgs[0]+"#"+msgs[1]+"#delete#" +msgs[3]);
                    Log.v("delete", "Valar Morghulis : Sending a delete request to " + msgs[1] + " from " + msgs[0]);
                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

                    Log.v("insert", "Waiting for co-ord ack  here................");
                    String message = (String) ois.readObject();
                    Log.v("insert", "Reply received as "+ message);
                    ois.close();
                    oos.close();
                    socket.close();

                } catch (UnknownHostException e) {

                    Log.e(TAG, "ClientTask UnknownHostException");
                }
                catch(SocketTimeoutException e)
                {
                    Log.e(TAG, "ClientTask SocketTimeOut from " + msgs[1] + " " + msgs[2]);
                    Log.e(TAG, e.getMessage());
                    deleteFromReplicas(msgs[1],msgs[3]);
                }
                catch (IOException e) {

                    Log.e(TAG, "ClientTask socket IOException");
                    //Log.e(TAG, e.getMessage());
                    deleteFromReplicas(msgs[1], msgs[3]);


                }
                catch(Exception ex)
                {
                    Log.e(TAG, "ClientTask socket Exception");
                    Log.e(TAG, ex.getMessage());

                }
            }
            else if(msgs[2].equals("deleteReplica"))
            {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    //PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    //sender#receiver#operation#key
                    oos.writeObject(msgs[0]+"#"+msgs[1]+"#deleteReplica#" +msgs[3]);
                    //out.println(msgs[0]+"#"+msgs[1]+"#deleteReplica#" +msgs[3]);
                    Log.v("delete", "Valar Morghulis: Sending a delete  replica request to " + msgs[1] + " from " + msgs[0]);
                    oos.close();
                    socket.close();

                } catch (UnknownHostException e) {

                    Log.e(TAG, "ClientTask UnknownHostException");
                }
                catch (IOException e) {

                    Log.e(TAG, "ClientTask socket IOException");
                    //Log.e(TAG, e.getMessage());
                }
            }
            else if(msgs[2].equals("giveMeKeysToReplicate"))
            {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    //PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    //sender#receiver#operation
                    oos.writeObject(msgs[0]+"#"+msgs[1]+"#giveMeKeysToReplicate");
                    //out.println(msgs[0]+"#"+msgs[1]+"#giveMeKeysToReplicate");
                    Log.v("deadComesAlive", "Get keys to replicate : Sending a request to  " + msgs[1] + " from " + msgs[0]);
                    oos.close();
                    socket.close();

                } catch (UnknownHostException e) {

                    Log.e(TAG, "ClientTask UnknownHostException");
                }
                catch (IOException e) {

                    Log.e(TAG, "ClientTask socket IOException");
                    //Log.e(TAG, e.getMessage());
                }
            }
            else if(msgs[2].equals("giveMeMyKeys"))
            {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));
                    //PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    //sender#receiver#operation#sender'sPredecessor
                    oos.writeObject(msgs[0]+"#"+msgs[1]+"#giveMeMyKeys#"+msgs[3]);
                    //out.println(msgs[0]+"#"+msgs[1]+"#giveMeMyKeys#"+msgs[3]);
                    Log.v("deadComesAlive", "Get my keys replicated in successors : Sending a request to  " + msgs[1] + " from " + msgs[0]);
                    oos.close();
                    socket.close();

                } catch (UnknownHostException e) {

                    Log.e(TAG, "ClientTask UnknownHostException");
                }
                catch (IOException e) {

                    Log.e(TAG, "ClientTask socket IOException");
                    //Log.e(TAG, e.getMessage());
                }
            }
            else if(msgs[2].equals("keysToInsert"))
            {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    //PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    //sender#receiver#operation#value
                    if (msgs[3] == null || msgs[3].length() == 0)
                    {
                        oos.writeObject(msgs[0]+"#"+msgs[1]+"#keysToInsert#null");

                    }
                    else {
                        oos.writeObject(msgs[0] + "#" + msgs[1] + "#keysToInsert#" + msgs[3]);
                    }
                    //out.println(msgs[0]+"#"+msgs[1]+"#keysToInsert#"+msgs[3]);
                    Log.v("deadComesAlive", "Giving keys to insert : Sending a request to  " + msgs[1] + " from " + msgs[0]);
                    oos.close();
                    socket.close();

                } catch (UnknownHostException e) {

                    Log.e(TAG, "ClientTask UnknownHostException");
                }
                catch (IOException e) {

                    Log.e(TAG, "ClientTask socket IOException");
                    //Log.e(TAG, e.getMessage());
                }
            }

            return null;

        }

    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}