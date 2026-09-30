package me.jaehyeon.model

// Java classes usually have a default constructor automatically, but not Kotlin data classes.
// Jackson expects a default way to instantiate objects unless you give it detailed instructions.
data class Order(
    val orderId: String = "",
    val bidTime: String = "",
    val price: Double = 0.0,
    val item: String = "",
    val supplier: String = "",
)
