case class Sale(product: String, quantity: Int, price: Double)

case class Customer(id: Int, name: String)

case class Order(orderId: Int, customerId: Int, product: String, quantity: Int)

object Main {

  def main(args: Array[String]): Unit = {

    // =========================================================
    // 1. SALES LIST - map, filter, flatMap and reduce
    // =========================================================

    println("=== 1. Sales List Processing ===")

    val sales = List(
      Sale("Laptop", 2, 50000),
      Sale("Mouse", 5, 800),
      Sale("Keyboard", 3, 1500),
      Sale("Monitor", 2, 12000),
      Sale("Headphones", 4, 2500)
    )

    println("Original Sales: " + sales)

    // map - calculate total value of each sale
    val saleValues = sales.map(sale =>
      sale.product -> (sale.quantity * sale.price)
    )

    println("Sale Values: " + saleValues)

    // filter - select sales where quantity is greater than 2
    val bulkSales = sales.filter(sale => sale.quantity > 2)

    println("Bulk Sales: " + bulkSales)

    // flatMap - create one entry for every unit sold
    val individualProducts = sales.flatMap(sale =>
      List.fill(sale.quantity)(sale.product)
    )

    println("Individual Products: " + individualProducts)

    // reduce - calculate total sales amount
    val totalSales = saleValues
      .map(_._2)
      .reduce((a, b) => a + b)

    println(f"Total Sales: ₹$totalSales%.2f")


    // =========================================================
    // 2. VECTOR - indexed customer records
    // =========================================================

    println("\n=== 2. Vector - Customer Records ===")

    val customers = Vector(
      Customer(1, "Arun"),
      Customer(2, "Priya"),
      Customer(3, "Rahul"),
      Customer(4, "Sneha")
    )

    println("Customers: " + customers)

    // Vector supports efficient indexed access
    println("Customer at index 0: " + customers(0))
    println("Customer at index 2: " + customers(2))

    println(
      "Vector is useful when we need an ordered collection " +
      "with fast indexed access."
    )


    // =========================================================
    // 3. MAP - PRODUCT QUANTITIES AND PRICES
    // =========================================================

    println("\n=== 3. Map - Product Quantities and Prices ===")

    val productQuantities = Map(
      "Laptop" -> 2,
      "Mouse" -> 5,
      "Keyboard" -> 3,
      "Monitor" -> 2,
      "Headphones" -> 4
    )

    val productPrices = Map(
      "Laptop" -> 50000.0,
      "Mouse" -> 800.0,
      "Keyboard" -> 1500.0,
      "Monitor" -> 12000.0,
      "Headphones" -> 2500.0
    )

    println("Product Quantities: " + productQuantities)
    println("Product Prices: " + productPrices)

    val productTotals = productQuantities.map {
      case (product, quantity) =>
        val price = productPrices(product)
        product -> (quantity * price)
    }

    println("Product Totals: " + productTotals)

    val mapTotalSales = productTotals.values.sum

    println(f"Total calculated using Map: ₹$mapTotalSales%.2f")


    // =========================================================
    // 4. FOR-COMPREHENSION - CUSTOMERS AND ORDERS
    // =========================================================

    println("\n=== 4. For-Comprehension ===")

    val orders = List(
      Order(101, 1, "Laptop", 1),
      Order(102, 2, "Mouse", 2),
      Order(103, 3, "Monitor", 1),
      Order(104, 1, "Keyboard", 1),
      Order(105, 4, "Headphones", 2)
    )

    println("Orders: " + orders)

    val customerOrders = for {
      customer <- customers
      order <- orders
      if customer.id == order.customerId
    } yield (customer.name, order.orderId, order.product, order.quantity)

    println("Customer Orders:")

    customerOrders.foreach {
      case (name, orderId, product, quantity) =>
        println(
          s"$name -> Order $orderId -> $product -> Quantity: $quantity"
        )
    }


    // =========================================================
    // 5. DAILY SALES SUMMARY - WITHOUT SPARK
    // =========================================================

    println("\n=== 5. Daily Sales Summary ===")

    val totalQuantity = sales
      .map(_.quantity)
      .reduce((a, b) => a + b)

    val averageSaleValue = totalSales / sales.size

    println("Number of Products: " + sales.size)
    println("Total Quantity Sold: " + totalQuantity)
    
    println("\nProduct-wise Revenue:")

    productTotals.foreach {
       case (product, amount) =>
         println(f"$product%-12s : ₹$amount%.2f")
    }

    println("-----------------------------")
    println(f"Total Revenue      : ₹$totalSales%.2f")
    println(f"Average Sale Value : ₹$averageSaleValue%.2f")



    println("\nDaily sales summary completed without Spark.")
  }
}