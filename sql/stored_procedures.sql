/*
    1. Procedimientos Almacenados definidos por el usuario
    2. Procedimientos Almacenados definidos por el sistema
        EXEC sp_addrolemember 
    3. Procedimientos Almacenados temporales
        LOCALES
            Están enlazados a la sesión y solo son accesibles por el usuario que los define
            Cuando se cierra la sesión se eliminan
            <schema>.#<nombre_procedimiento>
        GLOBALES
            Están enlazados a la sesión y son accesibles para todos los usuarios
            Se eliminan cuando termina la última sesión que lo esté utilizando
            <schema>.##<nombre_procedimiento>
    
    CREATE PROCEDURE <schema>.<nombre_procedimiento>
        @<nombre_parametro> <tipo_dato>,
        @<nombre_parametro> <tipo_dato> OUTPUT
    AS
    BEGIN
        -- Código a ejecutar
    END
*/
USE ejercicios_2024
GO

CREATE OR ALTER VIEW [dbo].[vw_detalle_operacion_completa] AS
SELECT
    do.detalle_operacion_id,
    eo.fecha AS fecha_encabezado,
    eo.codigo AS codigo_encabezado,
    eo.tipo_documento_id AS tipo_documento_encabezado_id,
    td.tipo_documento AS tipo_documento_encabezado,
    eo.numero_de_documento AS numero_documento_encabezado,
    eo.fecha_de_documento AS fecha_documento_encabezado,
    do.producto_id,
    p.codigo_de_barras AS codigo_de_barras_producto,
    p.nombre_producto AS nombre_producto,
    do.um_id,
    um.unidad_medida AS unidad_medida,
    do.cantidad,
    do.precio_unitario,
    do.monto_total,
    do.creado_en AS fecha_creacion_detalle
FROM
    inv.detalle_operacion do
INNER JOIN
    inv.encabezado_operacion eo ON do.encabezado_operacion_id = eo.encabezado_operacion_id
INNER JOIN
    inv.tipo_documento td ON eo.tipo_documento_id = td.tipo_documento_id
INNER JOIN
    inv.producto p ON do.producto_id = p.producto_id
INNER JOIN
    inv.unidad_medida um ON do.um_id = um.um_id;
GO


SELECT * FROM dbo.vw_detalle_operacion_completa
ORDER BY detalle_operacion_id DESC
OFFSET 10 ROWS
FETCH NEXT 10 ROWS ONLY
GO


CREATE OR ALTER PROCEDURE inv.sp_paginar_detalles_operacion (
    @pagina INT,
    @tamanioPagina INT
)
AS
BEGIN
    SELECT * FROM dbo.vw_detalle_operacion_completa
    ORDER BY detalle_operacion_id DESC
    OFFSET (@pagina - 1) * @tamanioPagina ROWS
    FETCH NEXT @tamanioPagina ROWS ONLY
END
GO

EXEC inv.sp_paginar_detalles_operacion 1, 10
EXEC inv.sp_paginar_detalles_operacion 2, 10
EXEC inv.sp_paginar_detalles_operacion 1, 5
EXEC inv.sp_paginar_detalles_operacion 2, 5
EXEC inv.sp_paginar_detalles_operacion 3, 5
EXEC inv.sp_paginar_detalles_operacion 4, 5

USE ecommerce
GO

CREATE OR ALTER PROCEDURE cli.sp_validar_tarjeta_vigente (
    @fechaBusqueda DATE,
    @clienteId INT 
) 
AS 
BEGIN
    DECLARE @cantidadTarjetasCliente INT;

    SELECT 
        @cantidadTarjetasCliente = COUNT(*)
    FROM cli.tarjetas_credito
    WHERE cliente_id = @clienteId

    IF @cantidadTarjetasCliente > 0 
    BEGIN
        SELECT
            tarjeta_id,
            numero_tarjeta,
            fecha_vencimiento
        FROM cli.tarjetas_credito
        WHERE cliente_id = @clienteId AND fecha_vencimiento > @fechaBusqueda
    END
    ELSE
    BEGIN
        SELECT CONCAT('El cliente: ', @clienteId, 'no posee tarjetas almacenadas') message
    END
END 

EXEC cli.sp_validar_tarjeta_vigente '2045-05-01', 1