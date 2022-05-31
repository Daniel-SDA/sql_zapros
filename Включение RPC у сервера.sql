
-----------------------------------------------------------------------------------------
/***************   Команда включения RPC и RPC OUT у заданного сервера ******************/
-----------------------------------------------------------------------------------------

EXEC master.dbo.sp_serveroption @server=N'192.168.10.3', @optname=N'rpc', @optvalue=N'true'
GO
EXEC master.dbo.sp_serveroption @server= N'192.168.10.3', @optname=N'rpc out', @optvalue=N'true'
GO