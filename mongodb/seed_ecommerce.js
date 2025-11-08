use('ecommerce')
db.dropDatabase()

// --- Catálogos y utilidades ---
const categories = ['Electrónica', 'Hogar', 'Moda', 'Deportes', 'Juguetes', 'Salud', 'Libros', 'Herramientas']
const brands = ['Acme', 'Globex', 'Umbrella', 'Wayne', 'Stark', 'Soylent', 'Wonka']
const cities = [
    { city: 'Guatemala City', loc: [-90.5133, 14.6349] },
    { city: 'Quetzaltenango', loc: [-91.518, 14.8347] },
    { city: 'Antigua', loc: [-90.7344, 14.5586] },
    { city: 'Escuintla', loc: [-90.785, 14.305] },
    { city: 'Cobán', loc: [-90.3708, 15.4691] },
]

function ri(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min
}
function pick(a) {
    return a[ri(0, a.length - 1)]
}

// --- Customers ---
const customers = []
for (let i = 0; i < 5000; i++) {
    const c = pick(cities)
    customers.push({
        name: `Nombre${i}`,
        last: `Apellido${i}`,
        email: `user${i}@demo.test`,
        phones: [`+502${ri(10000000, 99999999)}`],
        address: { line1: `Calle ${ri(1, 2000)} #${ri(1, 200)}`, city: c.city, location: { type: 'Point', coordinates: c.loc } },
        tags: Math.random() < 0.25 ? ['vip'] : [],
        createdAt: new Date(2022, ri(0, 11), ri(1, 28)),
    })
}
const custRes = db.customers.insertMany(customers)
const custIds = Object.values(custRes.insertedIds)
db.customers.createIndex({ email: 1 }, { unique: true })
db.customers.createIndex({ 'address.location': '2dsphere' })

// --- Products ---
const products = []
for (let i = 0; i < 2000; i++) {
    products.push({
        sku: `SKU${100000 + i}`,
        name: `Producto ${i}`,
        category: pick(categories),
        brand: pick(brands),
        price: ri(10, 1500) + 0.99,
        stock: ri(0, 500),
        description: `Descripción del producto ${i} con características variadas y útiles`,
        attrs: { color: pick(['negro', 'blanco', 'rojo', 'azul', 'verde']), sizes: ['S', 'M', 'L', 'XL'].slice(0, ri(1, 4)) },
    })
}
const prodRes = db.products.insertMany(products)
const prodIds = Object.values(prodRes.insertedIds)
db.products.createIndex({ category: 1, price: 1 })
db.products.createIndex({ name: 'text', description: 'text' })

// --- Orders y eventos ---
const statuses = ['created', 'paid', 'shipped', 'delivered', 'cancelled']
function makeOrder(i) {
    const cust = pick(custIds)
    const n = ri(1, 5)
    let items = []
    let total = 0
    for (let k = 0; k < n; k++) {
        const p = pick(prodIds)
        const qty = ri(1, 4)
        const price = db.products.findOne({ _id: p }, { price: 1 }).price // simple lookup
        total += price * qty
        items.push({ productId: p, qty: qty, price: price })
    }
    const createdAt = new Date(2023, ri(0, 11), ri(1, 28), ri(0, 23), ri(0, 59))
    const st = Math.random() < 0.08 ? 'cancelled' : Math.random() < 0.7 ? 'delivered' : pick(['paid', 'shipped'])
    return {
        customerId: cust,
        items: items,
        status: st,
        shippingAddressSnapshot: db.customers.findOne({ _id: cust }, { address: 1 }).address,
        total: Number(total.toFixed(2)),
        createdAt: createdAt,
        shippedAt: st !== 'cancelled' ? new Date(createdAt.getTime() + ri(1, 10) * 86400000) : null,
    }
}

let batch = []
let events = []
for (let i = 0; i < 30000; i++) {
    const o = makeOrder(i)
    batch.push(o)
    // eventos
    const oid = new ObjectId()
    o._id = oid
    const t0 = o.createdAt
    events.push({ orderId: oid, at: t0, type: 'created', payload: {} })
    if (o.status !== 'cancelled') {
        events.push({ orderId: oid, at: new Date(t0.getTime() + ri(1, 3) * 3600000), type: 'paid', payload: {} })
        events.push({ orderId: oid, at: new Date(t0.getTime() + ri(4, 36) * 3600000), type: 'shipped', payload: {} })
        if (o.status === 'delivered') {
            events.push({ orderId: oid, at: new Date(t0.getTime() + ri(48, 240) * 3600000), type: 'delivered', payload: {} })
        }
    } else {
        events.push({ orderId: oid, at: new Date(t0.getTime() + ri(1, 48) * 3600000), type: 'cancelled', payload: {} })
    }
    if (batch.length === 1000) {
        db.orders.insertMany(batch)
        batch = []
    }
    if (events.length >= 4000) {
        db.order_events.insertMany(events)
        events = []
    }
}
if (batch.length) db.orders.insertMany(batch)
if (events.length) db.order_events.insertMany(events)

db.orders.createIndex({ customerId: 1, createdAt: -1 })
db.orders.createIndex({ 'items.productId': 1 })
db.order_events.createIndex({ orderId: 1, at: 1 })
db.customers.createIndex({ 'address.location': '2dsphere' })
