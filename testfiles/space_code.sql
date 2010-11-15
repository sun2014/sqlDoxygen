select  a.TableName
  ,b.createtimestamp
  ,b.lastaltername
  ,b.lastaltertimestamp
  ,sum(a.CurrentPerm/(1024*1024*1024)) as ActualSpace
  ,(count(*)*(max(a.CurrentPerm)-avg(a.CurrentPerm)))/(1024*1024*1024) as WastedSpace
  , (ActualSpace + WastedSpace) as Total_Space
  
  from dbc.TableSize a 
  inner join dbc.tables b 
   on  a.databasename = b.databasename 
    and a.tablename = b.tablename
  where a.databaseName='PP_SCRATCH_gba' and b.lastaltername = 'krrathore' /*and a.tablename = 'your table name'   */
  group by 1,2,3,4
  order by Total_Space desc;
