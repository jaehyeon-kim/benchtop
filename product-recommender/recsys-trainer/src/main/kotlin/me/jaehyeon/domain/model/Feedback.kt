package me.jaehyeon.domain.model

import java.io.Serializable

/**
 * Domain entity representing a User's interaction (Feedback) with a Product.
 *
 * @property eventId Unique ID of the interaction.
 * @property productId The item that was recommended.
 * @property reward 1 for Click, 0 for No-Click.
 * @property contextVector The feature vector describing the context (User + Time) at the moment of recommendation.
 * @property timestamp Epoch timestamp of the event.
 */
data class Feedback(
    val eventId: String,
    val productId: String,
    val reward: Int,
    val contextVector: List<Double>,
    val timestamp: Long,
) : Serializable
