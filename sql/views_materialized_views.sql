/*
    CREAR UNA VISTA

    CREATE VIEW <Schema>.<Nombre Vista> AS
    SELECT * FROM ...
*/
CREATE VIEW sell.vw_productos_con_categoria AS
SELECT 
    p.*, 
    c.categoria 
FROM sell.productos p
JOIN sell.categoria c on p.categoria_id = c.categoria_id
GO

SELECT * FROM sell.vw_productos_con_categoria
GO


CREATE VIEW sell.vw_ventas_anio_mes_producto AS
SELECT 
    YEAR(v.fecha_venta) anio,
    MONTH(v.fecha_venta) mes,
    p.nombre_producto, 
    SUM(dv.cantidad * dv.precio_unitario) monto_total
FROM sell.detalle_ventas dv
JOIN sell.ventas v on dv.venta_id = v.venta_id 
JOIN sell.productos p on dv.producto_id = p.producto_id
GROUP BY YEAR(v.fecha_venta),  MONTH(v.fecha_venta), p.nombre_producto
GO


SELECT * FROM sell.vw_ventas_anio_mes_producto
where anio = 2024 
order by mes



DROP VIEW SELL.vw_productos_con_categoria

/*
RFM por cliente (Recencia, Frecuencia, Valor)

Enunciado. Construye una vista que calcule, por cliente:
    Recencia (R): días desde la última compra (MAX(fecha_venta)).
    Frecuencia (F): número de ventas.
    Valor (M): monto total comprado (suma de cantidad*precio_unitario).

La vista debe exponer cliente_id, nombre, apellido, recencia_dias, frecuencia_ventas, monto_total.
*/






