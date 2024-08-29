/*-- 1) Open database BMG_master */
/*-- 2) Attach database IPEDS */

alter table Bayview add COLUMN instnm TEXT;
alter table Bayview add COLUMN sector TEXT;
alter table Bayview add COLUMN iclevel TEXT;
alter table Bayview add COLUMN control TEXT;
alter table Bayview add COLUMN instsize TEXT;
alter table Bayview add COLUMN efydetot_tot_22 INTEGER;
alter table Bayview add COLUMN efyde_tot_22 INTEGER;
alter table Bayview add COLUMN typeinst TEXT;
alter table Bayview add COLUMN insttype TEXT;


WITH CTE as (
	SELECT b.IPEDID
		, i.field2 as instnm
		, i.field3 as sector
		, i.field4 as iclevel
		, i.field5 as control
		, i.field6 as instsize
		, i.field7 as efydetot_tot_22
		, i.field8 as efyde_tot_22
		, i.field9 as typeinst
		, i.field10 as insttype
	FROM Bayview b
	INNER JOIN IPEDS.hdic_dist_2022_selected i
	ON b.IPEDID = i.field1
)

UPDATE  Bayview
SET instnm = (SELECT instnm FROM CTE i WHERE Bayview.IPEDID = i.IPEDID)
	, sector = (SELECT sector FROM  CTE i WHERE Bayview.IPEDID = i.IPEDID)
	, iclevel = (SELECT iclevel FROM  CTE i WHERE Bayview.IPEDID = i.IPEDID)
	, control = (SELECT control FROM  CTE i WHERE Bayview.IPEDID = i.IPEDID)
	, instsize = (SELECT instsize FROM  CTE i WHERE Bayview.IPEDID = i.IPEDID)
	, efydetot_tot_22 = (SELECT efydetot_tot_22 FROM  CTE i WHERE Bayview.IPEDID = i.IPEDID)
	, efyde_tot_22 = (SELECT efyde_tot_22 FROM  CTE i WHERE Bayview.IPEDID = i.IPEDID)
	, typeinst = (SELECT typeinst FROM  CTE i WHERE Bayview.IPEDID = i.IPEDID)
	, insttype = (SELECT insttype FROM  CTE i WHERE Bayview.IPEDID = i.IPEDID)
;
