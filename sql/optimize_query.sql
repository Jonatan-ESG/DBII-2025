/* =====================================================================
   Clase: Optimización de Consultas en SQL Server (ECommerceDB)
   Autor: Ing. Jonatan Sandoval
   Propósito: Script DEMO único, comentado y ordenado para acompañar la clase.
   Modo de uso:
     1) Conéctate a la instancia que tenga ECommerceDB.
     2) Ejecuta sección por sección (Ctrl+E) con "Include Actual Execution Plan".
     3) Opcional: activa SET STATISTICS IO, TIME ON para medir.
   Nota: Este script crea algunos índices de apoyo "para demo".
         Al final hay una sección de limpieza (DROP INDEX IF EXISTS).
   ===================================================================== */

USE ECommerceDB;
GO
SET NOCOUNT ON;
-- Sugerencia: ve activando/desactivando estas métricas cuando quieras medir.
-- SET STATISTICS IO, TIME ON;
-- SET STATISTICS IO, TIME OFF;


/* =========================================================================================
   1) ORDEN LÓGICO DE UN SELECT (referencia rápida)
   FROM -> ON -> OUTER -> WHERE -> GROUP BY -> HAVING -> (window functions) -> SELECT
        -> DISTINCT -> ORDER BY -> TOP/OFFSET
   * El optimizador puede ejecutar físicamente en otro orden si conserva el mismo resultado.
   ========================================================================================= */

--------------------------------------------------------------------------------
-- 2) DEMO: FECHAS SARGABLES (de SCAN a SEEK con rangos)
--------------------------------------------------------------------------------
PRINT 'Demo 2: Fechas SARGables';

-- A) MAL: función sobre la columna (no usa índice por rango)
SELECT COUNT(*) AS ventas_2024_mal
FROM sell.ventas
WHERE YEAR(fecha_venta) = 2024; -- NO SARGABLE

-- B) BIEN: rango equivalente (permite SEEK)
SELECT COUNT(*) AS ventas_2024_bien
FROM sell.ventas
WHERE fecha_venta >= '20240101'
  AND fecha_venta <  '20250101'; -- SARGABLE

--------------------------------------------------------------------------------
-- 3) DEMO: LIKE y prefijos (comodín al inicio vs prefijo)
--------------------------------------------------------------------------------
PRINT 'Demo 3: LIKE; leading wildcard vs prefijo';

-- A) MAL: comodín al inicio (no Seek)
SELECT TOP (20) producto_id, nombre_producto
FROM sell.productos
WHERE nombre_producto LIKE '%Pro%'; -- NO SARGABLE

-- B) MEJOR: prefijo (puede Seek si hay índice por nombre_producto)
SELECT TOP (20) producto_id, nombre_producto
FROM sell.productos
WHERE nombre_producto LIKE 'Pro%'; -- POTENCIALMENTE SARGABLE

--------------------------------------------------------------------------------
-- 4) DEMO: CONVERSIONES IMPLÍCITAS (tipos desalineados)
--------------------------------------------------------------------------------
PRINT 'Demo 4: Conversiones implícitas';

-- A) MAL: variable NVARCHAR vs columna VARCHAR -> CONVERT_IMPLICIT + posible SCAN
DECLARE @mail_bad NVARCHAR(100) = N'user10@mail.com';
SELECT cliente_id, nombre, apellido
FROM cli.clientes
WHERE correo_electronico = @mail_bad; -- NO SARGABLE por conversión implícita

-- B) BIEN: tipos alineados (VARCHAR con VARCHAR)
DECLARE @mail_ok VARCHAR(100) = 'user10@mail.com';
SELECT cliente_id, nombre, apellido
FROM cli.clientes
WHERE correo_electronico = @mail_ok; -- SEEK esperado

-- C) ROBUSTO: columna calculada persistida para case-insensitive + índice (creación opcional)
IF COL_LENGTH('cli.clientes','correo_electronico_lc') IS NULL
BEGIN
    PRINT 'Creando columna calculada PERSISTED cli.clientes.correo_electronico_lc...';
    ALTER TABLE cli.clientes
      ADD correo_electronico_lc AS LOWER(correo_electronico) PERSISTED;
END
;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_clientes_correo_lc' AND object_id = OBJECT_ID('cli.clientes'))
BEGIN
    PRINT 'Creando índice IX_clientes_correo_lc...';
    CREATE INDEX IX_clientes_correo_lc ON cli.clientes(correo_electronico_lc);
END
;
DECLARE @mail_ci VARCHAR(100) = LOWER('USER10@mail.com');
SELECT cliente_id, nombre, apellido, correo_electronico
FROM cli.clientes
WHERE correo_electronico_lc = @mail_ci; -- SARGABLE y case-insensitive

--------------------------------------------------------------------------------
-- 5) DEMO: EXISTS vs IN vs JOIN con duplicación
--------------------------------------------------------------------------------
PRINT 'Demo 5: EXISTS vs IN vs JOIN';

-- A) IN (puede generar planes menos eficientes en algunos escenarios)
SELECT COUNT(DISTINCT v.cliente_id) AS clientes_con_compra_IN
FROM sell.ventas v
WHERE v.fecha_venta >= '20240101' AND v.fecha_venta < '20250101'
  AND v.cliente_id IN (SELECT c.cliente_id FROM cli.clientes c);

-- B) EXISTS (expresa existencia y suele optimizar mejor)
SELECT COUNT(*) AS clientes_con_compra_EXISTS
FROM cli.clientes c
WHERE EXISTS (
  SELECT 1
  FROM sell.ventas v
  WHERE v.cliente_id = c.cliente_id
    AND v.fecha_venta >= '20240101' AND v.fecha_venta < '20250101'
);

-- C) JOIN que multiplica filas -> obliga a DISTINCT (antipatrón para “existencia”)
SELECT DISTINCT c.cliente_id
FROM cli.clientes c
JOIN sell.ventas v         ON v.cliente_id = c.cliente_id
JOIN sell.detalle_ventas d ON d.venta_id   = v.venta_id; -- cuidado con el DISTINCT

--------------------------------------------------------------------------------
-- 6) DEMO: ORDER BY y cobertura (evitar SORT + Lookup)
--------------------------------------------------------------------------------
PRINT 'Demo 6: ORDER BY soportado por índice de cobertura';

-- A) Baseline: puede requerir SORT y Lookups
SELECT TOP (50) producto_id, nombre_producto, precio
FROM sell.productos
ORDER BY nombre_producto;

-- B) Crear índice que soporte orden y cubra columnas leídas
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_prod_nombre_cobertura' AND object_id = OBJECT_ID('sell.productos'))
BEGIN
    PRINT 'Creando índice IX_prod_nombre_cobertura...';
    CREATE INDEX IX_prod_nombre_cobertura
      ON sell.productos(nombre_producto)
      INCLUDE (precio, categoria_id);
END
;

-- C) Repetir consulta y observar menos costo/lecturas y ausencia de SORT/LOOKUP
SELECT TOP (50) producto_id, nombre_producto, precio
FROM sell.productos
ORDER BY nombre_producto;

--------------------------------------------------------------------------------
-- 7) DEMO: KEY LOOKUP (cuándo aparece y cómo mitigarlo)
--------------------------------------------------------------------------------
PRINT 'Demo 7: Key Lookup -> cobertura o reescritura';

-- A) Patrón propenso a Lookups
SELECT TOP (200)
       p.producto_id, p.nombre_producto, p.precio, p.stock, p.marca
FROM sell.productos p
WHERE p.categoria_id IS NULL
ORDER BY p.nombre_producto;

-- B) Índice de cobertura para eliminar Lookups (si procede)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_prod_cat_nombre_inc' AND object_id = OBJECT_ID('sell.productos'))
BEGIN
    PRINT 'Creando índice IX_prod_cat_nombre_inc...';
    CREATE INDEX IX_prod_cat_nombre_inc
      ON sell.productos(categoria_id, nombre_producto)
      INCLUDE (precio, stock, marca);
END
;

-- C) Repetir y comparar plan (idealmente sin Lookup)
SELECT TOP (200)
       p.producto_id, p.nombre_producto, p.precio, p.stock, p.marca
FROM sell.productos p
WHERE p.categoria_id IS NULL
ORDER BY p.nombre_producto;

--------------------------------------------------------------------------------
-- 8) DEMO: Agregaciones + filtro temprano (evitar funciones en la columna)
--------------------------------------------------------------------------------
PRINT 'Demo 8: Agregaciones con filtros sargables (rango por fecha)';

-- A) MAL: función sobre la columna
SELECT SUM(d.cantidad * d.precio_unitario) AS ingresos_2024_mal
FROM sell.detalle_ventas d
JOIN sell.ventas v ON v.venta_id = d.venta_id
WHERE YEAR(v.fecha_venta) = 2024; -- NO SARGABLE

-- B) BIEN: rango por fecha
SELECT SUM(d.cantidad * d.precio_unitario) AS ingresos_2024_bien
FROM sell.detalle_ventas d
JOIN sell.ventas v ON v.venta_id = d.venta_id
WHERE v.fecha_venta >= '20240101'
  AND v.fecha_venta <  '20250101'; -- SARGABLE

--------------------------------------------------------------------------------
-- 9) DEMO: Algoritmos de JOIN (Loops / Merge / Hash) *solo para docencia*
--------------------------------------------------------------------------------
PRINT 'Demo 9: Algoritmos de JOIN (solo con fines educativos; no usar hints en producción)';

-- Índices de apoyo para favorecer distintos planes
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_detalle_ventaid' AND object_id = OBJECT_ID('sell.detalle_ventas'))
BEGIN
    PRINT 'Creando índice IX_detalle_ventaid...';
    CREATE INDEX IX_detalle_ventaid ON sell.detalle_ventas(venta_id) INCLUDE (producto_id, cantidad, precio_unitario);
END
;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ventas_fecha' AND object_id = OBJECT_ID('sell.ventas'))
BEGIN
    PRINT 'Creando índice IX_ventas_fecha...';
    CREATE INDEX IX_ventas_fecha ON sell.ventas(fecha_venta) INCLUDE (cliente_id, total_venta);
END
;

-- a) Hash Join (robusto en grandes volúmenes sin orden)
SELECT SUM(d.cantidad * d.precio_unitario) AS ingresos_hash
FROM sell.detalle_ventas d
JOIN sell.ventas v ON v.venta_id = d.venta_id
WHERE v.fecha_venta >= '20240101'
  AND v.fecha_venta <  '20250101'
OPTION (HASH JOIN);

-- b) Merge Join (ideal si ya están ordenadas por la clave)
SELECT SUM(d.cantidad * d.precio_unitario) AS ingresos_merge
FROM sell.detalle_ventas d
JOIN sell.ventas v ON v.venta_id = d.venta_id
WHERE v.fecha_venta >= '20240101'
  AND v.fecha_venta <  '20250101'
OPTION (MERGE JOIN);

-- c) Nested Loops (eficiente cuando la externa es pequeña y hay Seek en la interna)
SELECT SUM(d.cantidad * d.precio_unitario) AS ingresos_loops
FROM sell.detalle_ventas d
JOIN sell.ventas v ON v.venta_id = d.venta_id
WHERE v.fecha_venta >= '20240101'
  AND v.fecha_venta <  '20250101'
OPTION (LOOP JOIN);

--------------------------------------------------------------------------------
-- 10) DEMO: Funciones de ventana y orden lógico (alias en WHERE NO, en ORDER BY SÍ)
--------------------------------------------------------------------------------
PRINT 'Demo 10: Orden lógico - alias y funciones de ventana';

-- (Referencia; NO ejecutar porque produce error)
-- WHERE items > 10 -- <== alias del SELECT no existe aún en WHERE

-- Correcto: usa la expresión o subconsulta/CTE
WITH ventas_clientes AS (
  SELECT v.cliente_id,
         SUM(d.cantidad) AS items
  FROM sell.ventas v
  JOIN sell.detalle_ventas d ON d.venta_id = v.venta_id
  WHERE v.fecha_venta >= '20240101' AND v.fecha_venta < '20250101'
  GROUP BY v.cliente_id
)
SELECT TOP (5) cliente_id, items
FROM ventas_clientes
WHERE items > 10
ORDER BY items DESC; -- aquí sí puedes usar el alias

-- Ventanas (se evalúan lógicamente antes de ORDER BY/DISTINCT)
SELECT TOP (10)
       v.cliente_id,
       SUM(d.cantidad) AS items,
       SUM(SUM(d.cantidad)) OVER (ORDER BY MIN(v.fecha_venta)) AS acumulado -- ejemplo simple
FROM sell.ventas v
JOIN sell.detalle_ventas d ON d.venta_id = v.venta_id
WHERE v.fecha_venta >= '20240101' AND v.fecha_venta < '20250101'
GROUP BY v.cliente_id;

--------------------------------------------------------------------------------
-- 11) LIMPIEZA OPCIONAL
--------------------------------------------------------------------------------
-- Descomenta estas líneas si deseas eliminar los índices creados solo para demo:

-- IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_prod_cat_nombre_inc' AND object_id = OBJECT_ID('sell.productos'))
-- DROP INDEX IX_prod_cat_nombre_inc ON sell.productos;

-- IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_prod_nombre_cobertura' AND object_id = OBJECT_ID('sell.productos'))
-- DROP INDEX IX_prod_nombre_cobertura ON sell.productos;

-- IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_detalle_ventaid' AND object_id = OBJECT_ID('sell.detalle_ventas'))
-- DROP INDEX IX_detalle_ventaid ON sell.detalle_ventas;

-- IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ventas_fecha' AND object_id = OBJECT_ID('sell.ventas'))
-- DROP INDEX IX_ventas_fecha ON sell.ventas;

-- IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_clientes_correo_lc' AND object_id = OBJECT_ID('cli.clientes'))
-- DROP INDEX IX_clientes_correo_lc ON cli.clientes;
