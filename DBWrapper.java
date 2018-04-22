package edu.upenn.cis.stormlite;

import java.io.File;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class DBWrapper
{
	private Environment env;

	private HashMap<String, Database> reduceBoltDb = new HashMap<String, Database>();
	private EnvironmentConfig envConfig = new EnvironmentConfig();
	private DatabaseConfig dbConfig = new DatabaseConfig();
	private StoredClassCatalog classCatalog;
	private Database classCatalogDb;
	public DBWrapper() 
	{

	}
	
	public void initialise(File envHome, boolean readOnly) throws DatabaseException, NoSuchAlgorithmException //dbhome,false
	{
		
		envConfig.setReadOnly(false);
		dbConfig.setReadOnly(false);

		envConfig.setAllowCreate(true);
		dbConfig.setAllowCreate(true);

		envConfig.setTransactional(true);
		dbConfig.setTransactional(true);
		
		env = new Environment(envHome, envConfig);
	
		classCatalogDb = env.openDatabase(null, "ClassCatalogDB", dbConfig);
		classCatalog = new StoredClassCatalog(classCatalogDb);
	}

	public void addReduceBoltDb(String id)//create the reduce db for the workers
	{
		// TODO Auto-generated method stub
		Database newDb = env.openDatabase(null, id, dbConfig);
		reduceBoltDb.put(id, newDb);
	}

	public HashMap<String, ArrayList<String>> getAllGroup(String id) 
	{
		// TODO Auto-generated method stub
		
		Database CurrentDb = reduceBoltDb.get(id);
		HashMap<String, ArrayList<String>> groups = new HashMap<String, ArrayList<String>>();
		Cursor cursor = CurrentDb.openCursor(null, null);
		DatabaseEntry foundKey = new DatabaseEntry();
		DatabaseEntry foundVal = new DatabaseEntry();
		EntryBinding dataBinding = new SerialBinding(classCatalog, SerializedList.class);
		
		while (cursor.getNext(foundKey, foundVal, LockMode.DEFAULT) == OperationStatus.SUCCESS)
		{
			String key = StringBinding.entryToString(foundKey);
			SerializedList values = (SerializedList)dataBinding.entryToObject(foundVal);
			groups.put(key, values.arr);
		}
		
		cursor.close();
		return groups;
	}

	public void addValue(String id, String key, String value) 
	{
		// TODO Auto-generated method stub
		Database CurrentDb = reduceBoltDb.get(id);
		
		DatabaseEntry keyEntry = new DatabaseEntry();
		DatabaseEntry dataEntry = new DatabaseEntry();
		StringBinding.stringToEntry(key, keyEntry);
		EntryBinding dataBinding = new SerialBinding(classCatalog, SerializedList.class);
		Cursor cursor = CurrentDb.openCursor(null, null);
		OperationStatus status = cursor.getSearchKey(keyEntry, dataEntry, LockMode.DEFAULT);
		cursor.close();
		
		if (status == OperationStatus.SUCCESS) //value already exists hence append
		{
			SerializedList values = (SerializedList)dataBinding.entryToObject(dataEntry);
			values.arr.add(value);
			
			dataBinding.objectToEntry(values, dataEntry);
			Transaction txn = env.beginTransaction(null, null);
			status = CurrentDb.put(txn, keyEntry, dataEntry);
			if (status != OperationStatus.SUCCESS) {
				txn.abort();
				System.out.println("Insert Failed");
			}
			txn.commit();
		} 
		else //first instance for that value
		{
			SerializedList values = new SerializedList();
			values.arr.add(value);
			dataBinding.objectToEntry(values, dataEntry);
			
			Transaction txn = env.beginTransaction(null, null);
			status = CurrentDb.put(txn, keyEntry, dataEntry);
			if (status != OperationStatus.SUCCESS) {
				txn.abort();
				System.out.println("Insert Failed");
			}
			txn.commit();
		}
		
	}
}