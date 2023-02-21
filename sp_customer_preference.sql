/****** Object:  StoredProcedure [consumption].[sp_customer_preference]    Script Date: 2/7/2023 12:52:28 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [consumption].[sp_customer_preference]
(
    @system_date datetime
)
AS
BEGIN
DECLARE @TranName VARCHAR(20);  
		SELECT @TranName = 'customer_preference';  
		BEGIN TRY
		BEGIN TRANSACTION @TranName

declare @report_date datetime;
	if (@system_date is null)
        begin
			set @system_date=DATEADD(hour,8 , SYSDATETIME())
        end
set @system_date=EOMONTH(DATEADD(MONTH,-1,@system_date));
set @report_date = cast(convert(char(8), @system_date, 112) + ' 23:59:59.99' as datetime);
truncate table [consumption].[customer_preference_temp] ;
insert into [consumption].[customer_preference_temp]  
select page_grouping,visit_id,visitor_id,
count(*) as n_clicks,
sum(page_time_spent) as total_time_spent,count(distinct(visit_id)) as number_of_visits ,
max(member_id) as member_id,
IIF(max(member_id) is NULL ,0,1) as is_member,
min(visit_date) as visit_date 
from [adobe].[app_lm1_lm2_silver] where visit_date<=@report_date and visit_date>=EOMONTH(DATEADD(Month,-13,@report_date))
group by visit_id,visitor_id,page_grouping ;

exec consumption.sp_customer_preference_page_grouping @report_date ;
exec consumption.sp_customer_preference_total @report_date;
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

