# DatasetCache

{:********************************************************************
*  NSDataCache  <br/>
*  all System  <br/>
*  @Date 11.01.2019 <br/>
*  Copyright 2019 by MicroObjects <br/>
*  @author Niek Sluijter <br/>
*  @Desc  Threadsafe Updatable Dataset Cache with indexes and db update functionality
**********************************************************************

This single file defines a cache system that can cache many tables on a memory efficient way
-Dynamicly add or delete fields
-Add Indexes (to random objects even)
-Update the database with values written to the cache (in a separate thread)
-Used in a Professional web application
-Can easily cache hunderds of records
**********************************************************************}



Example usage:


    qry:= TIBSQL.Create(Nil);
    qry.sql:= 'select * from customer';
    qry.open;

Or use my database adapter technology:

    q_cust := db.GetIQuery('select * from customer',[daoOneway]);//
    while q_cust.loop() do              // loop example
    q_cust.s['fieldname']:= 'string';   // assign example
    q_cust out of scope will automaticly free memory (its an interface)

Then create cache (list of objects)


    g_cacheUser:= TDatasetCacheTSLock.Create('user'); // create a cache dataset named user
    g_cacheUser.ClassRow:= TInvestigator;             // use specific class for objects (optional)
    g_cacheUser.Assign(iq.D,'USER_ID',True);          // assign whole dataset to it (automaticly copy fields
    g_cacheUser.AddIndex('name',true).Assign(g_cacheUser,'name;firstname');
    g_cacheUser.AddField('myfield',ftInteger);        // add custom field

Working with cache records (objects)

    mylist:= g_cacheUser.getList('name')

    while mylist.loop do begin
      s:= mylist.s['name'];
      mylist.S['testfield']:= 'bla';

      row:= mylist.current; // alternative
      row.S['testfield']:= 'bla';
    end;

    // also can add records
    newrow:= g_cacheUser.addRow('keyvalue');
    newrow.getIndex('test',True).addRow(row);

Internal structure

The list are based on tstringlists but in future should be changed into more efficient list.

The record data ist stored in a single string for efficiency

Sponser
