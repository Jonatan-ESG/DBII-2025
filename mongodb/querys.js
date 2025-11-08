// SELECCION, FILTRO Y OPERADORES LÓGICOS

// Productos de Electrónica entre Q500 y Q1,000 (500-^`999.99)
db.products.find({ category: 'Electrónica', price: { $gte: 500, $lt: 1000 } }, { name: 1, price: 1, _id: 0 })

// Órdenes recientes (últimos 30 días) entregadas o pagadas
db.orders.find(
    { createdAt: { $gte: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000) }, status: { $in: ['paid', 'delivered'] } },
    { _id: 1, customerId: 1, status: 1, createdAt: 1 }
)

// Productos de marca Acme o Stark, pero NO en categoría Moda
db.products.find(
    { $and: [{ brand: { $in: ['Acme', 'Stark'] } }, { category: { $not: { $eq: 'Moda' } } }] },
    { name: 1, brand: 1, category: 1, _id: 0 }
)

// Órdenes: (delivered OR shipped) AND total >= 1500
db.orders.find({ $and: [{ $or: [{ status: 'delivered' }, { status: 'shipped' }] }, { total: { $gte: 1500 } }] }, { _id: 1, total: 1, status: 1 })

// Clientes con "phones" presente y de tipo array
db.customers.find({ phones: { $exists: true, $type: 'array' } }, { name: 1, email: 1, phones: 1, _id: 0 })

// Órdenes que guardaron snapshot de dirección (campo embebido)
db.orders.find({ 'shippingAddressSnapshot.line1': { $exists: true } }, { _id: 1, 'shippingAddressSnapshot.city': 1 })

// Órdenes donde algún ítem tiene qty >= 3 del mismo producto
const p = db.products.findOne({}, { _id: 1 })._id
db.orders.find({ items: { $elemMatch: { productId: p, qty: { $gte: 3 } } } }, { _id: 1, items: { $slice: 3 } })

// Productos con tallas que incluyen M y L (ambas)
db.products.find({ 'attrs.sizes': { $all: ['M', 'L'] } }, { name: 1, 'attrs.sizes': 1, _id: 0 })

// Productos con EXACTAMENTE 4 tallas cargadas
db.products.find({ 'attrs.sizes': { $size: 4 } }, { name: 1, 'attrs.sizes': 1, _id: 0 })

// Un covered query se cumple si todos los campos usados en filtro y proyección están en el índice (y no proyectas nada más).

// Índice compuesto (crearlo una sola vez)
db.products.createIndex({ category: 1, price: 1 })

// Consulta "potencialmente cubierta": filtro y proyección usan solo category y price
db.products.find({ category: 'Electrónica', price: { $gte: 200, $lt: 400 } }, { _id: 0, category: 1, price: 1 })

// Si aquí añades name en la proyección, ya NO estaría cubierta porque name no está en el índice:
db.products.find(
    { category: 'Electrónica', price: { $gte: 200, $lt: 400 } },
    { _id: 0, category: 1, price: 1, name: 1 } // rompe el "covered"
)

// AGREGATION FRAMEWORK

db.orders.aggregate([
    { $match: { status: { $in: ['paid', 'shipped', 'delivered'] } } }, // filtra
    { $project: { customerId: 1, items: 1, createdAt: 1 } }, // reduce campos
    { $unwind: '$items' }, // explota array
    {
        $group: {
            // agrupa
            _id: '$customerId',
            revenue: { $sum: { $multiply: ['$items.price', '$items.qty'] } },
            orders: { $addToSet: '$_id' },
        },
    },
    { $sort: { revenue: -1 } }, // ordena
    { $limit: 10 }, // limita
])

// Enriquecer ítems con datos del producto (nombre, categoría)
db.orders.aggregate([
    { $match: { status: { $in: ['paid', 'shipped', 'delivered'] } } },
    { $unwind: '$items' },
    {
        $lookup: {
            from: 'products',
            localField: 'items.productId',
            foreignField: '_id',
            as: 'prod',
        },
    },
    { $set: { prod: { $first: '$prod' } } },
    {
        $project: {
            _id: 0,
            orderId: '$_id',
            createdAt: 1,
            qty: '$items.qty',
            unitPrice: '$items.price',
            product: '$prod.name',
            category: '$prod.category',
        },
    },
    { $limit: 10 },
])

// (a) AOV - Average Order Value (global y por ciudad)
db.products.aggregate([
    { $match: { price: { $gte: 200, $lte: 1200 } } },
    {
        $facet: {
            results: [{ $sort: { price: -1 } }, { $project: { _id: 0, name: 1, brand: 1, category: 1, price: 1 } }, { $limit: 20 }],
            byCategory: [{ $group: { _id: '$category', count: { $sum: 1 } } }, { $sort: { count: -1 } }],
            priceBands: [
                {
                    $bucket: {
                        groupBy: '$price',
                        boundaries: [0, 200, 500, 1000, 1500, 5000],
                        default: '1500+',
                        output: { count: { $sum: 1 } },
                    },
                },
            ],
        },
    },
])

//(b) Top productos por ingreso (30 días)
db.orders.aggregate([
    {
        $match: {
            status: { $in: ['paid', 'shipped', 'delivered'] },
            createdAt: { $gte: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000) },
        },
    },
    { $unwind: '$items' },
    {
        $group: {
            _id: '$items.productId',
            revenue: { $sum: { $multiply: ['$items.price', '$items.qty'] } },
            units: { $sum: '$items.qty' },
        },
    },
    { $lookup: { from: 'products', localField: '_id', foreignField: '_id', as: 'p' } },
    { $set: { p: { $first: '$p' } } },
    {
        $project: {
            _id: 0,
            productId: '$_id',
            name: '$p.name',
            category: '$p.category',
            revenue: { $round: ['$revenue', 2] },
            units: 1,
        },
    },
    { $sort: { revenue: -1 } },
    { $limit: 10 },
])

//(c) Recurrencia de clientes (repeat rate)
db.orders.aggregate([
    { $match: { status: { $in: ['paid', 'shipped', 'delivered'] } } },
    { $group: { _id: '$customerId', nOrders: { $sum: 1 } } },
    {
        $group: {
            _id: null,
            customersWithOrders: { $sum: 1 },
            customersWith2Plus: { $sum: { $cond: [{ $gte: ['$nOrders', 2] }, 1, 0] } },
        },
    },
    {
        $project: {
            _id: 0,
            repeatRate: { $round: [{ $divide: ['$customersWith2Plus', '$customersWithOrders'] }, 4] },
        },
    },
])

//(d) "Regresión de carritos" (proxy con order_events)
//Medimos drop-off: proporción de órdenes created que no llegan a paid en 24 h.
db.order_events.aggregate([
    { $match: { type: { $in: ['created', 'paid'] } } },
    {
        $group: {
            _id: '$orderId',
            types: { $addToSet: '$type' },
            firstCreated: { $min: { $cond: [{ $eq: ['$type', 'created'] }, '$at', null] } },
            firstPaid: { $min: { $cond: [{ $eq: ['$type', 'paid'] }, '$at', null] } },
        },
    },
    {
        $project: {
            hasPaid: { $in: ['paid', '$types'] },
            within24h: {
                $cond: [
                    { $and: [{ $ne: ['$firstPaid', null] }, { $ne: ['$firstCreated', null] }] },
                    { $lte: [{ $divide: [{ $subtract: ['$firstPaid', '$firstCreated'] }, 3600000] }, 24] },
                    false,
                ],
            },
        },
    },
    {
        $group: {
            _id: null,
            created: { $sum: 1 },
            paidIn24h: { $sum: { $cond: ['$within24h', 1, 0] } },
        },
    },
    {
        $project: {
            _id: 0,
            dropoffRate24h: { $round: [{ $subtract: [1, { $divide: ['$paidIn24h', '$created'] }] }, 4] },
        },
    },
])

// Búsqueda de productos con filtros simultáneos y facetas
db.products.aggregate([
    { $match: { price: { $gte: 200, $lte: 1200 } } },
    {
        $facet: {
            results: [{ $sort: { price: -1 } }, { $project: { _id: 0, name: 1, brand: 1, category: 1, price: 1 } }, { $limit: 20 }],
            byCategory: [{ $group: { _id: '$category', count: { $sum: 1 } } }, { $sort: { count: -1 } }],
            priceBands: [
                {
                    $bucket: {
                        groupBy: '$price',
                        boundaries: [0, 200, 500, 1000, 1500, 5000],
                        default: '1500+',
                        output: { count: { $sum: 1 } },
                    },
                },
            ],
        },
    },
])
