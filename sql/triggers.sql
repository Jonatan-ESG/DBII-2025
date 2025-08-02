/*
    Tablas disponibles según acción
        INSERT -> inserted, contiene todos los registros insertados de la tabla
        DELETE -> deleted, contiene todos los registros eliminados de la tabla 
        UPDATE -> inserted, contiene todos los registros actualizados de la tabla y  deleted, contiene todos los registros anteriores de la tabla 


    CREATE TRIGGER <schema_name>.<trigger_name> ON <table_schema_name>.<tabla_name>
    AFTER {[INSERT], [UPDATE], [DELETE]} AS 
    BEGIN

    END
*/


-- Trigger/Descadenador
CREATE OR ALTER TRIGGER inv.tgr_info_actualizacion_categoria ON inv.categoria
AFTER UPDATE AS
-- Acción 
BEGIN
    DECLARE @categoriaId INT, @cantidadProductosAfectados INT;

    SELECT 
        @categoriaId = categoria_id 
    FROM inserted;

    SELECT 
        @cantidadProductosAfectados = COUNT(*) 
    FROM inv.producto 
    WHERE categoria_id = @categoriaId;

    PRINT(CONCAT(@cantidadProductosAfectados, ' productos fueron afectados al ejecutar la actualización'))

END
GO

-- Evento
UPDATE inv.categoria
SET categoria = 'Nuevos Abrillantadores'
WHERE categoria_id =  3
GO

DROP TRIGGER inv.tgr_info_actualizacion_categoria
GO

CREATE OR ALTER TRIGGER inv.tgr_prevent_insersion_tipo_documento ON inv.tipo_documento
INSTEAD OF INSERT AS
BEGIN
    PRINT('No es posible insertar registros en esta tabla')
    INSERT INTO inv.tipo_documento (tipo_documento)
    VALUES ('NTD')
END
GO

INSERT INTO inv.tipo_documento (tipo_documento)
VALUES ('Nuevo tipo documento')
GO

DROP TRIGGER inv.tgr_prevent_insersion_tipo_documento
GO

ALTER TABLE inv.tipo_documento
ADD tipo_documento_cd VARCHAR(50)
GO

CREATE OR ALTER TRIGGER inv.tgr_agregar_codigo_tipo_documento ON inv.tipo_documento
INSTEAD OF INSERT AS
BEGIN
    INSERT INTO inv.tipo_documento (tipo_documento, tipo_documento_cd)
    SELECT 
        tipo_documento, 
        LEFT(UPPER(REPLACE(tipo_documento, ' ', '_')), 50)
    FROM inserted
END
GO

INSERT INTO inv.tipo_documento (tipo_documento)
VALUES ('Nuevo Tipo Tres'), ('Nuevo Tipo Dos')
GO

CREATE OR ALTER TRIGGER inv.tgr_actualiza_codigo_tipo_documento ON inv.tipo_documento
INSTEAD OF UPDATE AS
BEGIN

    UPDATE td SET 
        td.tipo_documento = i.tipo_documento,
        td.tipo_documento_cd = LEFT(UPPER(REPLACE(i.tipo_documento, ' ', '_')), 50)
    FROM inv.tipo_documento as td
    JOIN inserted as i on td.tipo_documento_id = i.tipo_documento_id
    
END
GO

UPDATE inv.tipo_documento
SET tipo_documento = 'Factura'
where tipo_documento_id = 2

SELECT * FROM inv.tipo_documento
GO

DISABLE TRIGGER inv.tgr_actualiza_codigo_tipo_documento ON inv.tipo_documento
GO

ENABLE TRIGGER inv.tgr_actualiza_codigo_tipo_documento ON inv.tipo_documento
GO

CREATE SCHEMA  system_logs 
GO

CREATE TABLE system_logs.insercion (
    fecha_creacion DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    registros VARCHAR(MAX)
)
GO

CREATE OR ALTER TRIGGER inv.tgr_insercion_categoria ON inv.categoria
AFTER INSERT AS
BEGIN
    INSERT INTO system_logs.insercion (registros)
    SELECT 
        CONCAT('Se agregó una nueva categoria con el ID: ', categoria_id, ' y la descripción: ', categoria) AS registro
    FROM inserted
END
GO

SELECT * FROM inv.categoria
GO

INSERT INTO inv.categoria (categoria)
VALUES ('Productos Varios')
GO

SELECT * FROM system_logs.insercion
GO

ALTER TABLE system_logs.insercion
ADD usuario VARCHAR(100) default ORIGINAL_LOGIN()
GO

INSERT INTO inv.categoria (categoria)
VALUES ('Descuentos Verano')
GO

SELECT * FROM system_logs.insercion
GO