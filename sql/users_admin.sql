CREATE SCHEMA sec
GO

/*
    LOGINS: Acceso a nivel del servidor
        CREATE LOGIN <nombre_login> WITH PASSWORD = ''
    USUARIOS: Acceso a nivel de la base de datos
        CREATE USER <nombre_usuario> FOR LOGIN <nombre_login>

    catalog_manager: administra catálogo (sell.productos, sell.categoria).
    sales_clerk: registra ventas (sell.ventas, sell.detalle_ventas).
    support_analyst: consulta clientes y ventas/carritos (solo lectura).
    finance_ro: solo lectura financiera (ventas totales), sin ver datos sensibles de tarjetas (cli.tarjetas_credito).

*/
USE master
GO
-- LOGINS (nivel servidor)
CREATE LOGIN catalog_manager WITH PASSWORD = 'C@tal0g#2025'
GO
CREATE LOGIN sales_clerk WITH PASSWORD = 'S@les#2025'
GO
CREATE LOGIN support_analyst WITH PASSWORD = 'Supp0rt#2025'
GO
CREATE LOGIN finance_ro WITH PASSWORD = 'F1nance#2025'
GO

-- USERS (nivel base de datos)
USE ecommerce
GO

CREATE USER catalog_manager FOR LOGIN catalog_manager
GO
CREATE USER sales_clerk FOR LOGIN sales_clerk
GO
CREATE USER support_analyst FOR LOGIN support_analyst
GO
CREATE USER finance_ro FOR LOGIN finance_ro
GO

-- Roles por dominio funcional
CREATE ROLE rol_catalogo -- CRUD sobre productos y categorias
GO
CREATE ROLE rol_ventas -- INSERT/UPDATE sobre ventas
GO
CREATE ROLE rol_soporte_ro -- Lectura clientes y ventas
GO
CREATE ROLE rol_finanzas_ro -- Lectura para finanzas (sin datos sensibles)
GO

-- Afiliación: usuarios a roles
EXEC sp_addrolemember rol_catalogo, catalog_manager
GO
EXEC sp_addrolemember rol_ventas, sales_clerk
GO
EXEC sp_addrolemember rol_soporte_ro, support_analyst
GO
EXEC sp_addrolemember rol_finanzas_ro, finance_ro
GO

-- ROL_CATALOGO
-- Esquema SELL: lectura general
GRANT SELECT ON SCHEMA::sell TO rol_catalogo
GO

-- Objectos del cátalogo: CRUD
GRANT SELECT, INSERT, UPDATE, DELETE ON sell.productos TO rol_catalogo
GO
GRANT SELECT, INSERT, UPDATE, DELETE ON sell.categoria TO rol_catalogo
GO

-- ROL_VENTAS
-- Lectura de prouctos para factura, y escritura en ventas

GRANT SELECT ON sell.productos TO rol_ventas -- permitir que vea precio/stock
GO
GRANT SELECT ON sell.categoria TO rol_ventas -- permitir que vea precio/stock
GO
GRANT SELECT, INSERT ON sell.ventas TO rol_ventas -- registrar ventas
GO
GRANT SELECT, INSERT ON sell.detalle_ventas TO rol_ventas
GO

-- Para corregir las cantidades
GRANT UPDATE ON sell.detalle_ventas TO rol_ventas
GO


-- ROL_SOPORTE_RO
-- Necesita ver clientes y ventas/carritos (solo lectura)
GRANT SELECT ON sell.ventas TO rol_soporte_ro
GO
GRANT SELECT ON sell.detalle_ventas TO rol_soporte_ro
GO
GRANT SELECT ON sell.carrito_compras TO rol_soporte_ro
GO
GRANT SELECT ON sell.detalle_carrito_compras TO rol_soporte_ro
GO
GRANT SELECT ON sell.productos TO rol_soporte_ro
GO
GRANT SELECT ON sell.categoria TO rol_soporte_ro
GO
GRANT SELECT ON cli.clientes TO rol_soporte_ro
GO
-- Bloqueo de datos sensibles (DENY prevalece sobre cualquier GRANT)
DENY SELECT ON cli.tarjetas_credito TO rol_soporte_ro
GO

-- ROL_FINANZAS_RO
-- Finanzas: lectura de ventas para reportes
GRANT SELECT ON sell.ventas TO rol_finanzas_ro
GO
GRANT SELECT ON sell.detalle_ventas TO rol_finanzas_ro
GO

-- BLoquear: clientes y tarjetas (principio de mínimo privilegio)
DENY SELECT ON cli.clientes TO rol_finanzas_ro
GO
DENY SELECT ON cli.tarjetas_credito TO rol_finanzas_ro
GO


-- Sales Clerk puede leer productos e insertar ventas
EXECUTE AS USER = 'sales_clerk'
-- Select correcto
SELECT TOP 3 producto_id, nombre_producto, precio FROM sell.productos
-- Select denegado
SELECT * FROM cli.clientes

--Insert correcto
INSERT INTO sell.ventas (cliente_id, fecha_venta, total_venta)
VALUES (1, GETDATE(), 500)
GO
-- Insert fallido
INSERT INTO sell.categoria(categoria) VALUES('otra categoria')
REVERT
GO

-- Finance sólo lectura de ventas; clientes/TC denegados
EXECUTE AS USER = 'finance_ro'
GO

-- Select correcto
SELECT TOP 3 * FROM sell.ventas
GO
-- Select fallido
SELECT TOP 3 * FROM cli.clientes
GO
-- Select fallido
SELECT TOP 3 * FROM cli.tarjetas_credito
GO
REVERT;
GO

-- Soporte: lectura clientes/ventas; tarjetas denegadas
EXECUTE AS USER = 'support_analyst'
GO

-- Select correcto
SELECT TOP 3 * FROM sell.ventas
GO
-- Select correcto
SELECT TOP 3 * FROM cli.clientes
GO
-- Select fallido
SELECT TOP 3 * FROM cli.tarjetas_credito
GO
REVERT;
GO


CREATE LOGIN mculajay WITH PASSWORD = 'Test123*'
GO

CREATE USER mculajay FOR LOGIN mculajay
GO

EXEC sp_addrolemember rol_catalogo, mculajay

GRANT SELECT ON cli.clientes TO mculajay
GO

EXECUTE AS USER = 'mculajay'
GO
-- SELECT * FROM sell.productos
SELECT * FROM cli.clientes
REVERT

EXECUTE AS USER = 'catalog_manager'
GO
-- SELECT * FROM sell.productos
SELECT * FROM cli.clientes
REVERT

