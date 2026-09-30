package me.jaehyeon.domain.model

import com.fasterxml.jackson.annotation.JsonProperty
import java.io.Serializable

/**
 * Domain entity representing the trained model parameters for a specific product.
 *
 * @property productId The product this model belongs to.
 * @property inverseA The inverse of the covariance matrix (A^-1). Stored to speed up inference.
 *                    Mapped to "A_inv" in JSON for compatibility with Python client.
 * @property b The reward vector.
 */
data class LinUCBModel(
    val productId: String,
    @get:JsonProperty("A_inv")
    val inverseA: List<List<Double>>,
    val b: List<Double>,
) : Serializable
