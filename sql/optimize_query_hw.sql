------------------------------------------------------------
-- a) Ingresos por cliente y mes (último año) 
------------------------------------------------------------
SELECT
    v.cliente_id,
    DATENAME(month, v.fecha_venta)  AS mes,     -- MAL (texto y función)
    YEAR(v.fecha_venta)             AS anio,    -- MAL (función)
    SUM(d.cantidad * d.precio_unitario) AS ingresos
FROM sell.ventas v
JOIN sell.detalle_ventas d ON d.venta_id = v.venta_id
WHERE YEAR(v.fecha_venta) = YEAR(GETDATE()) - 0  -- MAL (función)
GROUP BY v.cliente_id, DATENAME(month, v.fecha_venta), YEAR(v.fecha_venta)
ORDER BY ingresos DESC;                           -- sin Top/índice de soporte

------------------------------------------------------------
-- b) Top-N productos por facturación en rango 
------------------------------------------------------------
DECLARE @ini NVARCHAR(8) = N'20240101';
DECLARE @fin NVARCHAR(8) = N'20241231';

SELECT TOP (20)
       d.producto_id,
       SUM(d.cantidad * d.precio_unitario) AS ingresos
FROM sell.detalle_ventas d
WHERE d.venta_id IN (                                        -- MAL (IN masivo)
    SELECT v.venta_id
    FROM sell.ventas v
    WHERE CONVERT(NVARCHAR(8), v.fecha_venta, 112) >= @ini   -- MAL (CONVERT sobre la columna)
      AND CONVERT(NVARCHAR(8), v.fecha_venta, 112) <= @fin   -- MAL
)
GROUP BY d.producto_id
ORDER BY ingresos DESC;

------------------------------------------------------------
-- c) Tasa de conversión carrito → venta (mismo mes)
------------------------------------------------------------
SELECT
  (SELECT COUNT(DISTINCT v.cliente_id)
   FROM sell.ventas v
   WHERE FORMAT(v.fecha_venta, 'yyyyMM') = FORMAT(GETDATE(), 'yyyyMM')) -- MAL (FORMAT)
  * 1.0 /
  NULLIF((
    SELECT COUNT(DISTINCT cc.cliente_id)
    FROM sell.carrito_compras cc
    WHERE YEAR(cc.fecha_creacion) = YEAR(GETDATE())  -- MAL (función)
      AND MONTH(cc.fecha_creacion) = MONTH(GETDATE())-- MAL
      AND (cc.abandonado = 0 OR cc.abandonado IS NULL) -- filtro débil
  ), 0) AS tasa_conversion_mensual;
