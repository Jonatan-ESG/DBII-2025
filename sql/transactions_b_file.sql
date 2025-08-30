/*
	DIRTY READ (B = lector)
*/

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT 
	nombre_producto, 
	precio, 
	stock 
FROM sell.productos
WHERE codigo_barras = 'CMKNG-SZCNC';
-- IR A ARCHIVO A

SELECT 
	nombre_producto, 
	precio, 
	stock 
FROM sell.productos
WHERE codigo_barras = 'CMKNG-SZCNC';

/*
    NON-REPETABLE READ (B = escritor)
*/

UPDATE sell.productos 
SET precio = precio + 5
where codigo_barras = 'CMKNG-SZCNC'
-- IR A ARCHIVO A

UPDATE sell.productos 
SET precio = precio + 5
where codigo_barras = 'CMKNG-SZCNC'
-- IR A ARCHIVO A

/*
    PHANTOM READ (B = escritor)
*/

DECLARE @clienteId INT = (SELECT TOP 1 cliente_id FROM cli.clientes);

INSERT INTO sell.ventas(cliente_id, fecha_venta, total_venta)
VALUES (@clienteId, '2025-08-31', 100)
GO
-- IR A ARCHIVO A

SELECT 
    count(*) as ventas
FROM sell.ventas
WHERE fecha_venta = '2025-08-31'

-- IR A ARCHIVO A 

DECLARE @clienteId INT = (SELECT TOP 1 cliente_id FROM cli.clientes);

INSERT INTO sell.ventas(cliente_id, fecha_venta, total_venta)
VALUES (@clienteId, '2025-08-20', 100)
GO



SELECT 
    r.session_id, 
    r.status, 
    r.command, 
    r.wait_type, 
    r.wait_time, 
    r.blocking_session_id,
    DB_NAME(r.database_id) AS db, 
    t.text
FROM sys.dm_exec_requests r
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
ORDER BY r.blocking_session_id DESC, r.session_id;





