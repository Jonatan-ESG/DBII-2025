USE ecommerce
GO

/*
    EJERCICIO 1:
    Mostrar el total de compras por cliente (sin detalle), incluyendo el total histórico gastado por ese cliente.
*/

SELECT 
    v.venta_id,
    CONCAT(c.nombre, ' ', c.apellido) nombre_completo,
    v.fecha_venta,
    v.total_venta,
    SUM(v.total_venta) OVER (PARTITION BY v.cliente_id ORDER BY v.fecha_venta) AS historico_cliente
FROM sell.ventas v
JOIN cli.clientes c on c.cliente_id = v.cliente_id
where v.cliente_id = 1

/*
    EJERCICIO 2
    Asigne un ranking a las ventas en un periodo de tiempo según su venta total
*/

SELECT 
    venta_id,
    cliente_id,
    fecha_venta, 
    total_venta,
    RANK() OVER (ORDER BY total_venta DESC) as ranking_venta,
    DENSE_RANK() OVER (ORDER BY total_venta DESC) as ranking_venta_denso
FROM sell.ventas
WHERE fecha_venta BETWEEN '2021-01-01' and '2021-01-31' and cliente_id IN (274, 275)
ORDER BY fecha_venta ASC


/*
    EJERCICIO 3: 
    En base al promedio de compras de un cliente, determine si su comportamiento está fuera de lo usual
*/

SELECT 
    venta_id,
    cliente_id,
    fecha_venta, 
    total_venta,
    AVG(total_venta) OVER (PARTITION BY cliente_id) AS promedio_compras,
    CASE 
        WHEN total_venta > AVG(total_venta) OVER (PARTITION BY cliente_id) AND  ((total_venta / AVG(total_venta) OVER (PARTITION BY cliente_id))-1) > 0.8 THEN 'Posiblemente Fraudulento'
        ELSE 'Normal'
    END AS es_fraudulento,
    FORMAT((total_venta / AVG(total_venta) OVER (PARTITION BY cliente_id)) - 1, 'P')  porcentaje_diferencia
FROM sell.ventas
WHERE fecha_venta BETWEEN '2021-01-01' and '2021-01-31' and cliente_id IN (274, 275, 276)
ORDER BY fecha_venta ASC

/*
    Numberar las compras por cada cliente según cuando compró
*/
WITH ventas_ordenadas AS (    
    SELECT 
        venta_id,
        cliente_id,
        fecha_venta, 
        total_venta,
        ROW_NUMBER() OVER ( PARTITION BY cliente_id ORDER BY fecha_venta, venta_id) numero_compra
    FROM sell.ventas
    WHERE fecha_venta BETWEEN '2021-01-01' and '2021-01-31' and cliente_id IN (274, 275)
)
select * FROM ventas_ordenadas
where numero_compra = 1