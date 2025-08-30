/*
    ATOMICIDAD
        error - rollback
*/
SET XACT_ABORT ON
GO

DECLARE @clienteId INT = (select top 1 cliente_id from cli.clientes);

DECLARE @productoId INT;

SELECT 
    @productoId =  producto_id
FROM sell.productos 
WHERE codigo_barras = 'CMKNG-SZCNC';

BEGIN TRY
    BEGIN TRAN;
        DECLARE @cantidad INT  = 99;
        DECLARE @precio DECIMAL(10,2) = (SELECT precio FROM sell.productos WHERE producto_id = @productoId);
        DECLARE @stock INT = (SELECT stock FROM sell.productos WHERE producto_id = @productoId);

        INSERT INTO sell.ventas(cliente_id, fecha_venta, total_venta)
        VALUES (@clienteId, CURRENT_TIMESTAMP, 0.00)

        DECLARE @ventaId INT = SCOPE_IDENTITY();

        INSERT INTO sell.detalle_ventas (venta_id, producto_id, cantidad, precio_unitario)
        VALUES (@ventaId, @productoId, @cantidad, @precio);

        UPDATE sell.ventas 
        SET total_venta = @cantidad * @precio
        WHERE venta_id = @ventaId;

        UPDATE sell.productos 
        SET stock = stock - @cantidad
        WHERE producto_id = @productoId;

        IF @stock < @cantidad 
            THROW 50010, 'Stock insuficiente', 1;

    COMMIT;
END TRY
BEGIN CATCH
    ROLLBACK;    
    SELECT 
        ERROR_MESSAGE() message, 
        ERROR_NUMBER() number;
END CATCH

/*
    SAVE POINT
*/

BEGIN TRAN;
UPDATE sell.productos SET stock = 0 WHERE codigo_barras = 'CMKNG-SZCNC';

SAVE TRAN VersionUno;

select * from sell.productos WHERE codigo_barras = 'CMKNG-SZCNC'

UPDATE sell.productos SET stock = stock + 1 WHERE codigo_barras = 'CMKNG-SZCNC';

select * from sell.productos WHERE codigo_barras = 'CMKNG-SZCNC'

ROLLBACK TRAN VersionUno;

COMMIT;

select * from sell.productos WHERE codigo_barras = 'CMKNG-SZCNC';

/*
    DIRTY READ (A = escritor)
*/

BEGIN TRAN;
UPDATE sell.productos SET stock = stock + 5, precio = 1500 WHERE codigo_barras = 'CMKNG-SZCNC';
-- IR A ARCHIVO B
ROLLBACK;
-- IR A ARCHIVO B

/*
    NON-REPETABLE READ (A = lector)
*/

SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

BEGIN TRAN;
SELECT 
    precio AS precio_1
FROM sell.productos 
WHERE codigo_barras = 'CMKNG-SZCNC'

-- IR A ARCHIVO B

SELECT 
    precio AS precio_2
FROM sell.productos 
WHERE codigo_barras = 'CMKNG-SZCNC'

COMMIT;


SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;

BEGIN TRAN;
SELECT 
    precio AS precio_1
FROM sell.productos 
WHERE codigo_barras = 'CMKNG-SZCNC'

-- IR A ARCHIVO B

SELECT 
    precio AS precio_2
FROM sell.productos 
WHERE codigo_barras = 'CMKNG-SZCNC'

COMMIT;


/*
    PHANTOM READ (A = lector)
*/
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;

BEGIN TRAN;
SELECT 
    count(*) as ventas
FROM sell.ventas
WHERE fecha_venta = '2025-08-31'

-- IR A ARCHIVO B
SELECT 
    count(*) as ventas
FROM sell.ventas
WHERE fecha_venta = '2025-08-31'

COMMIT


IF NOT EXISTS (
  SELECT 1 FROM sys.indexes WHERE name='IX_ventas_fecha' AND object_id=OBJECT_ID('sell.ventas')
)
    CREATE INDEX IX_ventas_fecha ON sell.ventas(fecha_venta);
GO



SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN TRAN;
SELECT 
    count(*) as ventas
FROM sell.ventas
WHERE fecha_venta >= '2025-08-01' and fecha_venta < '2025-08-30'

COMMIT;



/*
    Obeservabilidad (ver bloques/esperas)
*/

-- Proceso bloqueados
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


-- Visualización de sesiones
select * from  sys.dm_exec_sessions;

-- Más información
-- Lost update (patrón seguro con UPDLOCK)
-- SNAPSHOT y conflicto de actualización (3960)