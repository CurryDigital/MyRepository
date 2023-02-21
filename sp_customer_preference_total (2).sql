/****** Object:  StoredProcedure [consumption].[sp_customer_preference_total]    Script Date: 12/2/2022 6:43:05 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE OR ALTER PROCEDURE [consumption].[sp_customer_preference_total]
(
    @system_date datetime
)
AS
BEGIN
DECLARE @TranName VARCHAR(20);  
		SELECT @TranName = 'customer_preference_total';  
		BEGIN TRY
		BEGIN TRANSACTION @TranName
declare @report_date datetime;
	if (@system_date is null)
        begin
			set @system_date=DATEADD(hour,8 , SYSDATETIME())
        end
set @system_date=EOMONTH(DATEADD(MONTH,-1,@system_date));
set @report_date = cast(convert(char(8), @system_date, 112) + ' 23:59:59.99' as datetime);
delete from [consumption].[customer_preference_total] where year(@report_date)=report_year and month(@report_date)=report_month ;
insert into [consumption].[customer_preference_total]
Select month(@report_date) as report_month,
year(@report_date) as report_year,
'All' as page_grouping,
count(distinct(iif(month(A.visit_date)=month(@report_date) and year(A.visit_date)=year(@report_date),A.visit_id,NULL))) as n_visit_month,
SUM(iif (month(A.visit_date)=month(@report_date) and year(A.visit_date)=year(@report_date),A.total_time_spent,0)) as time_spent_month,
count(distinct(iif (month(A.visit_date)=month(@report_date) and year(A.visit_date)=year(@report_date),A.visitor_id +A.member_id,NULL)))as n_visitor_month,
SUM(iif(month(A.visit_date)=month(@report_date) and year(A.visit_date)=year(@report_date),A.n_clicks,0)) as n_clicks_month,
count(distinct(iif (month(A.visit_date)=month(@report_date) and year(A.visit_date)=year(@report_date),B.primary_member_id,NULL)))as n_unique_member_month,
count(distinct(iif (year(A.visit_date)=year(@report_date),A.visit_id,NULL))) as n_visit_ytd,
SUM(iif(year(A.visit_date)=year(@report_date),total_time_spent,0)) as time_spent_ytd,
count(distinct(iif (year(A.visit_date)=year(@report_date),visitor_id +A.member_id,NULL)))as n_visitor_ytd,
SUM(iif( year(A.visit_date)=year(@report_date),n_clicks,0)) as n_clicks_ytd,
count(distinct(iif (year(A.visit_date)=year(@report_date),B.primary_member_id,NULL)))as n_unique_member_ytd,
count(distinct(iif (month(A.visit_date)=month(@report_date) and year(A.visit_date)=year(@report_date)-1,visit_id,NULL))) as n_visit_last_year,
SUM(iif(month(A.visit_date)=month(@report_date) and year(A.visit_date)=year(@report_date)-1,total_time_spent,0)) as time_spent_last_year,
count(distinct(iif (month(A.visit_date)=month(@report_date) and year(visit_date)=year(@report_date)-1,visitor_id +A.member_id,NULL)))as n_visitor_last_year,
SUM(iif( month(A.visit_date)=month(@report_date) and year(A.visit_date)=year(@report_date)-1,n_clicks,0)) as n_clicks_last_year,
count(distinct(iif (month(A.visit_date)=month(@report_date) and year(visit_date)=year(@report_date)-1,B.primary_member_id,NULL)))as n_unique_member_last_year,
C.Membership_Tier as member_tier,
A.is_member as is_member,
B.is_new_member as is_new_member,
B.place_of_residence,
DATEADD(hour,8 , SYSDATETIME()) as last_modified_date
from 
(select visit_id,visitor_id,
count(case when page_grouping is not NULl then 1 end ) as n_clicks,
sum(page_time_spent) as total_time_spent,
--count(distinct(visit_id)) as number_of_visits ,
max(member_id) as member_id,
IIF(max(member_id) is NULL ,0,1) as is_member,
min(visit_date) as visit_date from [adobe].[app_lm1_lm2_silver]  where  visit_date<=@report_date and visit_date>=EOMONTH(DATEADD(Month,-13,@report_date))
group by visit_id,visitor_id)A
left join
(select member_id,  
iif(year(reg_date)=year(@report_date) and month(reg_date)=month(@report_date),1,0 )as is_new_member ,
 iif(address_country in ('HongKong','China','Macau'),address_country,'others')  as place_of_residence,isnull(primary_member_id,member_id) as primary_member_id from [hklhkcustomerretail].[ods_dim_sf_account_member] where member_id is not null and primary_member_id is NULL
 UNION ALL
 Select temp1.member_id,temp2.is_new_member,temp2.place_of_residence,temp1.primary_member_id from(select member_id,primary_member_id from [hklhkcustomerretail].[ods_dim_sf_account_member] temp where  member_id is not null and primary_member_id is NOT  NULL) temp1 
 inner join
 (select member_id,  
iif(year(reg_date)=year(@report_date) and month(reg_date)=month(@report_date),1,0 )as is_new_member ,
 iif(address_country in ('HongKong','China','Macau'),address_country,'others')  as place_of_residence,isnull(primary_member_id,member_id) as primary_member_id from [hklhkcustomerretail].[ods_dim_sf_account_member] where member_id is not null and primary_member_id is NULL) temp2 
 on temp1.primary_member_id=temp2.member_id
) B
on A.[member_id]=B.member_id 
 left  join
 (Select member_id,calculated_member_tier as membership_Tier from c360.member_tier_history_per_month  where report_month=month(@report_date) and report_year=year(@report_date)) C on A.member_id=C.member_id 
group by B.place_of_residence,B.is_new_member,C.Membership_Tier,A.is_member;

--==========================================
--==== Error Handling 
--==========================================
			COMMIT TRANSACTION @TranName
		END TRY
		BEGIN CATCH
			ROLLBACK TRANSACTION @TranName
			DECLARE @ErrorMessage NVARCHAR(4000);
			DECLARE @ErrorSeverity INT;
			DECLARE @ErrorState INT;

			SELECT 
			@ErrorMessage = ERROR_MESSAGE(),
			@ErrorSeverity = ERROR_SEVERITY(),
			@ErrorState = ERROR_STATE();

		
			RAISERROR (@ErrorMessage, -- Message text.
				@ErrorSeverity, -- Severity.
				@ErrorState -- State.
				)
		END CATCH



end

GO

