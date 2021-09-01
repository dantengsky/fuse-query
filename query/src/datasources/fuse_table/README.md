
For v0.5
- Let's use parquet format to save data blocks 
- table level OCC style transaction 
    - read-committed isolation
    - blind append only
- registered as `LocalTable` as well as `RemoteTable`, so that testing could be much easier
- ...
