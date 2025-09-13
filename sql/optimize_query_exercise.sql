------------------------------------------------------------
-- 1) Filtro por fecha NO sargable (función sobre la columna)
------------------------------------------------------------
SELECT COUNT(*) AS ventas_2024
FROM sell.ventas
WHERE YEAR(fecha_venta) = 2024; -- MAL

------------------------------------------------------------
-- 2) Texto NO sargable (función sobre la columna)
------------------------------------------------------------
SELECT producto_id, nombre_producto
FROM sell.productos
WHERE LEFT(nombre_producto, 4) = 'Prod'; -- MAL

------------------------------------------------------------
-- 3) Conversión implícita (variable NVARCHAR contra columna VARCHAR)
------------------------------------------------------------
DECLARE @mail NVARCHAR(100) = N'user10@mail.com';
SELECT cliente_id, nombre, apellido
FROM cli.clientes
WHERE correo_electronico = @mail; -- MAL (CONVERT_IMPLICIT)

------------------------------------------------------------
-- 4) JOIN que duplica filas y obliga a DISTINCT
------------------------------------------------------------
SELECT DISTINCT c.cliente_id
FROM cli.clientes c
JOIN sell.ventas v            ON v.cliente_id = c.cliente_id
JOIN sell.detalle_ventas d    ON d.venta_id   = v.venta_id; -- MAL (DISTINCT por join multiplicador)

------------------------------------------------------------
-- 5) Búsqueda con comodín al inicio (no usa índice) + ordenamiento caro
------------------------------------------------------------
SELECT TOP (100) producto_id, nombre_producto, precio
FROM sell.productos
WHERE nombre_producto LIKE '%Pro%'   -- MAL (leading wildcard)
ORDER BY UPPER(nombre_producto);     -- MAL (función en ORDER BY)

------------------------------------------------------------
-- 6) Patrón propenso a Key Lookups (muchas columnas, filtro poco selectivo)
------------------------------------------------------------
SELECT TOP (200)
       p.producto_id, p.nombre_producto, p.precio, p.stock, p.marca, p.descripcion
FROM sell.productos p
WHERE p.categoria_id IS NULL      -- MAL (filtro débil)
ORDER BY p.nombre_producto;       -- posible Lookup + Sort

------------------------------------------------------------
-- 7) Agregación con JOIN y función sobre la fecha (no sargable)
------------------------------------------------------------
SELECT SUM(d.cantidad * d.precio_unitario) AS ingresos_2024
FROM sell.detalle_ventas d
JOIN sell.ventas v ON v.venta_id = d.venta_id
WHERE YEAR(v.fecha_venta) = YEAR(GETDATE()); -- MAL

------------------------------------------------------------
-- 8) Búsqueda case-insensitive aplicando función a la columna
------------------------------------------------------------
DECLARE @q NVARCHAR(100) = N'USER30@MAIL.COM';
SELECT cliente_id, nombre, apellido, correo_electronico
FROM cli.clientes
WHERE LOWER(correo_electronico) = LOWER(@q); -- MAL (función sobre la columna)
