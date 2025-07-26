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

UPDATE inv.tipo_documento
SET tipo_documento = 'Nota de debito'
where tipo_documento_id = 3

select * from inv.tipo_documento