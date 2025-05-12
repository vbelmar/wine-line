const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const pgp = require('pg-promise')();
const mqtt = require('mqtt');

require('dotenv').config();

const app = express();
app.use(cors());
app.use(bodyParser.json());

app.use(express.static('public'));

// PostgreSQL connection
const db = pgp({
  user: process.env.DB_USER || 'postgres',
  password: process.env.DB_PASSWORD || '4916',
  host: process.env.DB_HOST || 'localhost',
  port: 5432,
  database: process.env.DB_NAME || 'GDI'
});


// MQTT connection
const BROKER_URL    = 'mqtt://mqtt.dsic.upv.es:1883';
const TOPIC_DB      = 'PR2/A7/db';
const TOPIC_ROBODK  = 'PR2/A7/robodk';
const TOPIC_CARO    = 'PR2/A7/cantidades/caro';
const TOPIC_BARATO  = 'PR2/A7/cantidades/barato';
const mqttOpts = {
  username: process.env.MQTT_USERNAME || 'giirob',
  password: process.env.MQTT_PASSWORD || 'UPV2024'
}

const mqttClient = mqtt.connect(BROKER_URL, mqttOpts);
mqttClient.on('connect', () => {
  console.log(`📡 Connected to MQTT broker at ${BROKER_URL}`);
});
mqttClient.on('error', (err) => {
  console.error(`❌ MQTT connection error: ${err.message}`);
});

let orderId         = null;
let remainingCaros  = null;
let remainingBaratos= null;
let packingSent     = false;

// Subscribe with QoS 0 (at most once)
mqttClient.subscribe(TOPIC_DB, { qos: 0 }, (err, granted) => {
  if (err) {
    console.error('Subscribe error:', err);
  } else {
    console.log('Subscribed to:', granted);
  }
});
mqttClient.subscribe(TOPIC_ROBODK, { qos: 0 }, (err, granted) => {
  if (err) {
    console.error('Subscribe error:', err);
  } else {
    console.log('Subscribed to:', granted);
  }
});
mqttClient.subscribe(TOPIC_CARO, { qos: 0 }, (err, granted) => {
  if (err) {
    console.error('Subscribe error:', err);
  } else {
    console.log('Subscribed to:', granted);
  }
});
mqttClient.subscribe(TOPIC_BARATO, { qos: 0 }, (err, granted) => {
  if (err) {
    console.error('Subscribe error:', err);
  } else {
    console.log('Subscribed to:', granted);
  }
});

mqttClient.on('message', (topic, payload) => {
  console.log(`← [${topic}]: ${payload.toString()}`);
});

function updateOrderStatus(status) {
  db.none('UPDATE orders SET estado = $1 WHERE id = $2', [status, orderId])
    .then(() => {
      console.log(`✅ Order ${orderId} marked as ${status}`);
      orderId = null;  // reset orderId after processing
    })
    .catch(err => {
      console.error(`❌ Error updating order status: ${err.message}`);
    });
}

// Ensure tables exist before accepting requests
(async () => {
  try {
    await db.none(`
      CREATE TABLE IF NOT EXISTS orders (
        id SERIAL PRIMARY KEY,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);

    await db.none(`
      CREATE TABLE IF NOT EXISTS order_items (
        id SERIAL PRIMARY KEY,
        order_id INTEGER REFERENCES orders(id) ON DELETE CASCADE,
        wine_type VARCHAR(255) NOT NULL,
        quantity INTEGER NOT NULL CHECK (quantity > 0)
      );
    `);

    console.log("✅ Tables verified or created.");
  } catch (err) {
    console.error("❌ Error checking/creating tables:", err.message);
  }
})();

mqttClient.on('message', (topic, payloadBuf) => {
  let payload;
  try {
    payload = JSON.parse(payloadBuf.toString());
  } catch(e) {
    console.error('Invalid JSON on', topic, payloadBuf.toString());
    return;
  }

  switch(topic) {
    case TOPIC_CARO:
      remainingCaros = payload.caro;
      console.log(`🔴 Remaining caros: ${remainingCaros}`);
      break;

    case TOPIC_BARATO:
      remainingBaratos = payload.barato;
      console.log(`🔵 Remaining baratos: ${remainingBaratos}`);
      break;

    case TOPIC_ROBODK:
      // e.g. { command: "finished", id_pedido: 123 }
      if (payload.id_pedido === orderId && payload.command === 'finished') {
        console.log('🏁 Simulator reports finished packing');
        updateOrderStatus('finished');
      }
      return;  // no need to check counts

    default:
      return;
  }

  // After updating counts, check if both are zero
  if (!packingSent
      && remainingCaros === 0
      && remainingBaratos === 0
  ) {
    console.log('📦 Both counts zero → marking packing');
    updateOrderStatus('packing');
    packingSent = true;
  }
});

app.post('/api/order', async (req, res) => {
  const { order } = req.body;

  if (!order || !Array.isArray(order) || order.length === 0) {
    return res.status(400).json({ message: 'Pedido vacío o malformado' });
  }

  try {
    console.log("📦 Received order:", order);

    await db.tx(async t => {
      console.log("📥 Inserting order...");
      const orderId = await t.one('INSERT INTO orders DEFAULT VALUES RETURNING id');
      console.log("✅ New order ID:", orderId.id);
    
      let total_caro = 0;
      let total_barato = 0;

      for (const item of order) {
        console.log("🧾 Inserting item:", item);
        try {
          await t.none(
            'INSERT INTO order_items (order_id, wine_type, quantity) VALUES ($1, $2, $3)',
            [orderId.id, item.tipo, item.cantidad]
          );
        } catch (err) {
          console.error(`❌ Failed to insert item: ${JSON.stringify(item)} →`, err.message);
          throw err; // triggers rollback
        }

        if (item.tipo === 'VINO DE LA CASA' || item.tipo === 'GRAN CAPITANA') {
          total_caro += item.cantidad;
        } else if (item.tipo === 'PEQUEÑA CRIANZA' || item.tipo === 'LA TRUCHA') {
          total_barato += item.cantidad;
        }
      }

      const orderMsg_barato = {
        id_pedido: orderId.id,
        total_barato: total_barato
      };

      const orderMsg_caro = {
        id_pedido: orderId.id,
        total_caro: total_caro,
      };

      console.log(total_caro, total_barato);

      mqttClient.publish(TOPIC_CARO, JSON.stringify(orderMsg_caro), { qos: 1, retain: false }, err => {
        if (err) console.error('Failed to send order to esp32', err);
        else     console.log(`🤖 Order sent to esp32:`, orderMsg_caro);
      });
      mqttClient.publish(TOPIC_BARATO, JSON.stringify(orderMsg_barato), { qos: 1, retain: false }, err => {
        if (err) console.error('Failed to send order to esp32', err);
        else     console.log(`🤖 Order sent to esp32:`, orderMsg_barato);
      });
    });

    const payload = JSON.stringify({order, timestamp: new Date()});
    /*mqttClient.publish(mqttTopic, payload, { qos: 1 }, err => {
      if (err) {
        console.error(`❌ Failed to publish message to MQTT: ${err.message}`);
      } else {
        console.log(`📤 Published message to MQTT topic ${mqttTopic}:`, payload);
      }
    });*/

    res.json({ message: 'Pedido registrado correctamente.' });
  } catch (err) {
    console.error("❌ Detailed DB error:", err);
    res.status(500).json({ message: 'Error al registrar el pedido.', details: err.message });
  }
});

app.get('/api/orders', async (req, res) => {
  try {
    const rows = await db.any(`
      SELECT o.id, o.fecha_pedido, o.estado, oi.wine_type, oi.quantity
      FROM orders o
      JOIN order_items oi ON oi.order_id = o.id
      ORDER BY 
        CASE 
          WHEN o.estado = 'completado' THEN 0
          ELSE 1
        END,
        o.fecha_pedido DESC;
    `);

    const groupedOrders = [];
    let currentOrderId = null;
    let currentOrder = null;

    rows.forEach(row => {
      if (row.id !== currentOrderId) {
        currentOrder = {
          id: row.id,
          fecha_pedido: row.fecha_pedido,
          estado: row.estado,
          items: []
        };
        groupedOrders.push(currentOrder);
        currentOrderId = row.id;
      }

      currentOrder.items.push({
        wine_type: row.wine_type,
        quantity: row.quantity
      });
    });

    res.json({ orders: groupedOrders });

  } catch (err) {
    console.error("❌ Error fetching orders:", err);
    res.status(500).json({ message: 'Error al obtener los pedidos.', details: err.message });
  }
});


app.get('/api/test-db', async (req, res) => {
  try {
    const result = await db.any('SELECT * FROM orders LIMIT 5');
    res.json({ message: 'DB connection OK', data: result });
  } catch (err) {
    console.error('❌ DB test failed:', err);
    res.status(500).json({ message: 'DB test failed', details: err.message });
  }
});

app.get('/api/mqtt-status', (req, res) => {
  res.json({ connected: mqttClient.connected });
});

const PORT = 3000;
app.listen(PORT, () => {
  console.log(`🚀 Server running at http://localhost:${PORT}`);
});