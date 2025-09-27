USE EcommerceDB;
GO

/* ================================
   A) PREPARACIÓN
==================================*/

-- A.1) Asegurar Query Store (ajusta a tu realidad)
ALTER DATABASE ecommerce_full SET QUERY_STORE = ON;
ALTER DATABASE ecommerce_full SET QUERY_STORE (OPERATION_MODE = READ_WRITE);
ALTER DATABASE ecommerce_full SET QUERY_STORE (INTERVAL_LENGTH_MINUTES = 15);
ALTER DATABASE ecommerce_full SET QUERY_STORE (CLEANUP_POLICY = (STALE_QUERY_THRESHOLD_DAYS = 7));
ALTER DATABASE ecommerce_full SET QUERY_STORE (MAX_STORAGE_SIZE_MB = 1024);
GO

-- A.2) Esquema para pruebas de carga / telemetría local
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'load')
    EXEC('CREATE SCHEMA load AUTHORIZATION dbo;');
GO

-- A.3) Tabla opcional para inserts concurrentes (simular OLTP ligero)
IF OBJECT_ID('load.cola_eventos') IS NULL
BEGIN
  CREATE TABLE load.cola_eventos
  (
      id            BIGINT IDENTITY(1,1) PRIMARY KEY,
      producto_id   INT NOT NULL,
      cantidad      INT NOT NULL,
      precio_unit   DECIMAL(18,4) NOT NULL,
      creado_utc    DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
      procesado     BIT NOT NULL DEFAULT 0
  );
  CREATE INDEX IX_cola_eventos_producto ON load.cola_eventos(producto_id) INCLUDE(cantidad, precio_unit, procesado);
END
GO

-- A.4) Función generadora de números pseudoaleatorios determinísticos (para T-SQL puro)
IF OBJECT_ID('load.ufn_rand_int') IS NOT NULL DROP FUNCTION load.ufn_rand_int;
GO
CREATE FUNCTION load.ufn_rand_int(@seed INT, @max INT)
RETURNS INT
AS
BEGIN
    -- LCG simple; no criptográfico. Útil para reproducibilidad.
    DECLARE @n BIGINT = (1103515245 * (@seed & 0x7fffffff) + 12345) & 0x7fffffff;
    RETURN CAST(@n % NULLIF(@max,0) AS INT);
END;
GO

/* ================================
   B) PROCEDIMIENTOS para SQLQueryStress
   (pensados para usar parámetros aleatorios y provocar planes distintos)
==================================*/

-- B.1) Ventas por producto (ya usado para “parameter sniffing”)
IF OBJECT_ID('dbo.sp_reporte_ventas_producto') IS NOT NULL
    DROP PROCEDURE dbo.sp_reporte_ventas_producto;
GO
CREATE PROCEDURE dbo.sp_reporte_ventas_producto
  @ProductoId INT
AS
BEGIN
  SET NOCOUNT ON;

  -- Antipatrones intencionales posibles para discusión (descomentar en clase):
  -- OPTION (RECOMPILE);            -- mitiga sniffing pero aumenta compilaciones
  -- OPTION (OPTIMIZE FOR UNKNOWN); -- plan “promedio”

  SELECT TOP (1000)
         dv.producto_id,
         SUM(dv.cantidad) AS unidades,
         SUM(dv.cantidad*dv.precio_unitario) AS ingreso
  FROM sell.detalle_ventas dv
  WHERE dv.producto_id = @ProductoId
  GROUP BY dv.producto_id
  ORDER BY ingreso DESC;
END
GO

-- B.2) Inserción concurrente (uso con 10–50 hilos)
IF OBJECT_ID('load.sp_insertar_eventos_aleatorios') IS NOT NULL
    DROP PROCEDURE load.sp_insertar_eventos_aleatorios;
GO
CREATE PROCEDURE load.sp_insertar_eventos_aleatorios
  @n INT = 100,
  @seed INT = 42
AS
BEGIN
  SET NOCOUNT ON;
  DECLARE @i INT = 0;

  WHILE @i < @n
  BEGIN
     DECLARE @pid INT = ABS(CHECKSUM(NEWID())) % 50000 + 1;
     DECLARE @qty INT = ABS(CHECKSUM(@i, @seed)) % 5 + 1;
     DECLARE @price DECIMAL(18,4) = (ABS(CHECKSUM(@pid)) % 5000) / 10.0 + 1;

     INSERT INTO load.cola_eventos(producto_id, cantidad, precio_unit)
     VALUES(@pid, @qty, @price);

     SET @i += 1;
  END
END
GO

-- B.3) Consulta pesada con ventanas + agregaciones mensuales
IF OBJECT_ID('dbo.sp_ventas_mes_categoria') IS NOT NULL
    DROP PROCEDURE dbo.sp_ventas_mes_categoria;
GO
CREATE PROCEDURE dbo.sp_ventas_mes_categoria
  @Desde DATE,
  @Hasta DATE
AS
BEGIN
  SET NOCOUNT ON;

  ;WITH base AS (
    SELECT
      DATEFROMPARTS(YEAR(v.fecha_venta), MONTH(v.fecha_venta), 1) AS mes,
      p.categoria_id,
      dv.producto_id,
      dv.cantidad * dv.precio_unitario AS ingreso
    FROM sell.ventas v
    JOIN sell.detalle_ventas dv ON dv.venta_id = v.venta_id
    JOIN sell.productos p       ON p.producto_id = dv.producto_id
    WHERE v.fecha_venta >= @Desde AND v.fecha_venta < DATEADD(DAY,1,@Hasta)
  ),
  agg AS (
    SELECT mes, categoria_id,
           SUM(ingreso) AS ingreso,
           COUNT_BIG(*) AS registros
    FROM base
    GROUP BY mes, categoria_id
  )
  SELECT
    a.*,
    SUM(ingreso) OVER (PARTITION BY categoria_id ORDER BY mes ROWS UNBOUNDED PRECEDING) AS ingreso_acum,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ingreso) OVER (PARTITION BY categoria_id) AS p95_categoria
  FROM agg a
  ORDER BY categoria_id, mes;
END
GO

-- B.4) Búsqueda no sargable + join (para forzar scans)
IF OBJECT_ID('dbo.sp_busqueda_texto_ingresos') IS NOT NULL
    DROP PROCEDURE dbo.sp_busqueda_texto_ingresos;
GO
CREATE PROCEDURE dbo.sp_busqueda_texto_ingresos
  @q NVARCHAR(50)
AS
BEGIN
  SET NOCOUNT ON;
  SELECT TOP (500)
         p.producto_id,
         p.nombre_producto,
         SUM(dv.cantidad*dv.precio_unitario) AS ingreso
  FROM sell.productos p
  JOIN sell.detalle_ventas dv ON dv.producto_id = p.producto_id
  WHERE p.nombre_producto LIKE N'%' + @q + N'%'
  GROUP BY p.producto_id, p.nombre_producto
  ORDER BY ingreso DESC;
END
GO

/* ================================
   C) CONSULTAS HEAVY READ
==================================*/

-- C.1) Ranking por cliente y mes (ventanas + ordenamientos)
IF OBJECT_ID('dbo.sp_top_clientes_mes') IS NOT NULL
    DROP PROCEDURE dbo.sp_top_clientes_mes;
GO
CREATE PROCEDURE dbo.sp_top_clientes_mes
  @Desde DATE,
  @Hasta DATE,
  @TopN INT = 5
AS
BEGIN
  SET NOCOUNT ON;
  ;WITH ventas AS (
    SELECT
      v.cliente_id,
      DATEFROMPARTS(YEAR(v.fecha_venta), MONTH(v.fecha_venta), 1) AS mes,
      dv.cantidad*dv.precio_unitario AS ingreso
    FROM sell.ventas v
    JOIN sell.detalle_ventas dv ON dv.venta_id = v.venta_id
    WHERE v.fecha_venta >= @Desde AND v.fecha_venta < DATEADD(DAY,1,@Hasta)
  ),
  agg AS (
    SELECT cliente_id, mes, SUM(ingreso) AS ingreso
    FROM ventas
    GROUP BY cliente_id, mes
  ),
  rnk AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY mes ORDER BY ingreso DESC) AS rn
    FROM agg
  )
  SELECT *
  FROM rnk
  WHERE rn <= @TopN
  ORDER BY mes, rn;
END
GO

-- C.2) Consulta con CROSS APPLY (top N por categoría)
IF OBJECT_ID('dbo.sp_topN_por_categoria') IS NOT NULL
    DROP PROCEDURE dbo.sp_topN_por_categoria;
GO
CREATE PROCEDURE dbo.sp_topN_por_categoria
  @Desde DATE,
  @Hasta DATE,
  @TopN INT = 10
AS
BEGIN
  SET NOCOUNT ON;

  ;WITH base AS (
    SELECT p.categoria_id, dv.producto_id, SUM(dv.cantidad*dv.precio_unitario) AS ingreso
    FROM sell.detalle_ventas dv
    JOIN sell.ventas v   ON v.venta_id = dv.venta_id
    JOIN sell.productos p ON p.producto_id = dv.producto_id
    WHERE v.fecha_venta BETWEEN @Desde AND @Hasta
    GROUP BY p.categoria_id, dv.producto_id
  )
  SELECT b.categoria_id, x.producto_id, x.ingreso
  FROM (SELECT DISTINCT categoria_id FROM base) b
  CROSS APPLY (
      SELECT TOP (@TopN) producto_id, ingreso
      FROM base
      WHERE categoria_id = b.categoria_id
      ORDER BY ingreso DESC
  ) AS x
  ORDER BY b.categoria_id, x.ingreso DESC;
END
GO


/* Devuelve el top 10 de esperas "relevantes" por categoria */

;WITH waits AS (
  SELECT wait_type,
         wait_time_ms/1000.0 AS wait_s,
         signal_wait_time_ms/1000.0 AS signal_s,
         100.0*signal_wait_time_ms/NULLIF(wait_time_ms,0) AS signal_pct
  FROM sys.dm_os_wait_stats
  WHERE wait_type NOT IN ('SLEEP_TASK','SLEEP_SYSTEMTASK','BROKER_TASK_STOP','BROKER_TO_FLUSH',
    'SQLTRACE_BUFFER_FLUSH','XE_TIMER_EVENT','XE_DISPATCHER_WAIT','BROKER_EVENTHANDLER',
    'FT_IFTS_SCHEDULER_IDLE_WAIT','BROKER_RECEIVE_WAITFOR','HADR_FILESTREAM_IOMGR_IOCOMPLETION',
    'HADR_WORK_QUEUE','HADR_CLUSAPI_CALL','DIRTY_PAGE_POLL','SP_SERVER_DIAGNOSTICS_SLEEP',
    'XE_LIVE_TARGET_TVF','LOGMGR_QUEUE','CHECKPOINT_QUEUE','DISPATCHER_QUEUE_SEMAPHORE',
    'CLR_AUTO_EVENT','SOS_WORK_DISPATCHER','QDS_ASYNC_QUEUE','QDS_PERSIST_TASK_MAIN_LOOP_SLEEP',
    'ONDEMAND_TASK_QUEUE','SQLTRACE_INCREMENTAL_FLUSH_SLEEP')
)
SELECT TOP (10) *, 
  CASE 
    WHEN wait_type LIKE 'PAGEIOLATCH%' THEN 'IO'
    WHEN wait_type='WRITELOG' THEN 'LOG'
    WHEN wait_type LIKE 'LCK[_]%' THEN 'LOCKS'
    WHEN wait_type IN ('CXPACKET','CXCONSUMER') THEN 'PARALLEL'
    WHEN wait_type='SOS_SCHEDULER_YIELD' OR signal_pct>25 THEN 'CPU'
    ELSE 'OTROS'
  END AS categoria
FROM waits
ORDER BY wait_s DESC;

/* I/O (ACCESO AL DISCO) por archivo, latencia de transacciones entre data/log */

SELECT DB_NAME(vfs.database_id) AS db_name, mf.name AS file_name,
       vfs.num_of_reads,
       vfs.io_stall_read_ms,
       CASE WHEN vfs.num_of_reads>0 THEN 1.0*vfs.io_stall_read_ms/vfs.num_of_reads END AS ms_per_read,
       vfs.num_of_writes,
       vfs.io_stall_write_ms,
       CASE WHEN vfs.num_of_writes>0 THEN 1.0*vfs.io_stall_write_ms/vfs.num_of_writes END AS ms_per_write
FROM sys.dm_io_virtual_file_stats(DB_ID(), NULL) vfs
JOIN sys.master_files mf ON vfs.database_id=mf.database_id AND vfs.file_id=mf.file_id
ORDER BY COALESCE(1.0*vfs.io_stall_read_ms/vfs.num_of_reads,0)
       + COALESCE(1.0*vfs.io_stall_write_ms/vfs.num_of_writes,0) DESC;


/* captura waits en vivo + síntomas (tempdb, spills, grants). */

EXEC sp_BlitzFirst @Seconds=15, @ExpertMode=1;


/* Carga A (Lectura con parameter sniffing) */

EXEC dbo.sp_reporte_ventas_producto 1;

EXEC sp_BlitzFirst @Seconds=15, @ExpertMode=1;   -- waits en vivo
EXEC sp_BlitzCache @SortOrder='cpu', @Top=20;    -- top CPU
EXEC sp_BlitzCache @SortOrder='reads', @Top=20;  -- top lecturas
EXEC sp_BlitzWho;                                -- sesiones activas/bloqueos
