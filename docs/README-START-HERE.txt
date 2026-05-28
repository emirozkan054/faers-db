FAERS-DB Windows App
====================

This app runs locally on your Windows computer. It does not need Python and it does not upload your searches anywhere.

First-time setup
----------------

1. Extract FAERS-DB-Windows.zip.
2. Extract the warehouse data zip.
3. Put the extracted folder named "warehouse" beside FAERS-DB.exe.
   Example:
   FAERS-DB\
     FAERS-DB.exe
     warehouse\
       demo.parquet
       drug.parquet
       ...
       warehouse-manifest.json
4. Double-click FAERS-DB.exe.
5. Your browser should open automatically.
6. Keep the black launcher window open while using the app. Close it when finished.

If the app says data is missing
-------------------------------

Check that the folder is named exactly "warehouse" and that it is beside FAERS-DB.exe.

Alternative location:
%LOCALAPPDATA%\FAERS-DB\warehouse

Updating the data
-----------------

1. Close FAERS-DB.
2. Delete or rename the old warehouse folder.
3. Extract the new warehouse data zip.
4. Put the new warehouse folder beside FAERS-DB.exe.
5. Launch FAERS-DB again.
