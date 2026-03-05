# Consultas API para calcular promos – Item MLA1674851553

Todas usan el token en el header:
```
Authorization: Bearer {ACCESS_TOKEN}
Accept: application/json
```

---

## 1. Sale Price (precio de venta + si tiene promo)

**Función:** `ml_get_item_sale_price_full`

```
GET https://api.mercadolibre.com/items/MLA1674851553/sale_price?context=channel_marketplace
```

- **Headers:** `Authorization: Bearer {ACCESS_TOKEN}`, `Accept: application/json`
- **Respuesta:** `amount`, `regular_amount`, `promotion_id`, `promotion_type`
- Si `regular_amount != amount` → hay promo; si trae `promotion_id` y `promotion_type` se usa en el punto 2.

---

## 2. Descuentos por item (cuando sale_price SÍ devuelve promotion_id)

**Función:** `ml_get_promotion_item_discounts`

### 2a. Items de la promo

Reemplazá `{PROMOTION_ID}` y `{PROMOTION_TYPE}` con los valores de la respuesta del paso 1:

```
GET https://api.mercadolibre.com/seller-promotions/promotions/{PROMOTION_ID}/items?promotion_type={PROMOTION_TYPE}&item_id=MLA1674851553&app_version=v2
```

- **Headers:** `Authorization: Bearer {ACCESS_TOKEN}`, `Accept: application/json`
- **Respuesta:** `results[]` → `meli_percentage`/`meli_percent`, `seller_percentage`/`seller_percent` por item

### 2b. Fallback: detalle de la promo (si 2a no trae meli/seller por item)

```
GET https://api.mercadolibre.com/seller-promotions/promotions/{PROMOTION_ID}?promotion_type={PROMOTION_TYPE}&app_version=v2
```

- **Respuesta:** `benefits` → `meli_percent`, `seller_percent`

---

## 3. Cuando sale_price NO devuelve promotion_id (fallback)

**Función:** `ml_get_promotion_item_discounts_by_user`

### 3a. Promos del usuario

Reemplazá `{USER_ID}` con tu seller_id numérico:

```
GET https://api.mercadolibre.com/seller-promotions/users/{USER_ID}?app_version=v2
```

- **Respuesta:** `results[]` con promos → `id`, `type`, `benefits`

### 3b. Por cada promo: ver si MLA1674851553 está incluido

```
GET https://api.mercadolibre.com/seller-promotions/promotions/{PROMOTION_ID}/items?promotion_type={PROMOTION_TYPE}&item_id=MLA1674851553&app_version=v2
```

---

## Otras (precio sin promo)

**sale_price (solo amount):**
```
GET https://api.mercadolibre.com/items/MLA1674851553/sale_price?context=channel_marketplace
```

**prices (fallback):**
```
GET https://api.mercadolibre.com/items/MLA1674851553/prices
```

---

## Para probar en Búsqueda (pegar y ejecutar)

```
GET https://api.mercadolibre.com/items/MLA1674851553/sale_price?context=channel_marketplace
```

```
GET https://api.mercadolibre.com/items/MLA1674851553/prices
```

*(Para 2a, 2b y 3b hay que usar `PROMOTION_ID` y `PROMOTION_TYPE` de la respuesta del paso 1 o 3a.)*

---

## Curl (reemplazá TU_TOKEN y los IDs de promo/usuario)

```bash
# 1. Sale price – MLA1674851553
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://api.mercadolibre.com/items/MLA1674851553/sale_price?context=channel_marketplace"

# 2a. Items de una promo (PROMOTION_ID y STANDARD desde paso 1)
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://api.mercadolibre.com/seller-promotions/promotions/PROMOTION_ID/items?promotion_type=STANDARD&item_id=MLA1674851553&app_version=v2"

# 2b. Detalle de la promo
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://api.mercadolibre.com/seller-promotions/promotions/PROMOTION_ID?promotion_type=STANDARD&app_version=v2"

# 3a. Promos del usuario
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://api.mercadolibre.com/seller-promotions/users/USER_ID?app_version=v2"

# 3b. MLA1674851553 en una promo (PROMOTION_ID y tipo desde 3a)
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://api.mercadolibre.com/seller-promotions/promotions/PROMOTION_ID/items?promotion_type=STANDARD&item_id=MLA1674851553&app_version=v2"

# Precios (fallback)
curl -H "Authorization: Bearer TU_TOKEN" \
  "https://api.mercadolibre.com/items/MLA1674851553/prices"
```
