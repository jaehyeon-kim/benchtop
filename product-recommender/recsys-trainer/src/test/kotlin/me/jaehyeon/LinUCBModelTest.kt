package me.jaehyeon

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import me.jaehyeon.domain.model.LinUCBModel
import kotlin.test.Test
import kotlin.test.assertEquals

class LinUCBModelTest {
    @Test
    fun `the model is written with the field names the Python client reads`() {
        val model = LinUCBModel(productId = "7", inverseA = listOf(listOf(1.0, 0.0), listOf(0.0, 1.0)), b = listOf(0.5, 0.0))

        val json = jacksonObjectMapper().readTree(jacksonObjectMapper().writeValueAsString(model))

        assertEquals(setOf("productId", "A_inv", "b"), json.fieldNames().asSequence().toSet())
        assertEquals(1.0, json["A_inv"][0][0].asDouble())
        assertEquals(0.5, json["b"][0].asDouble())
    }
}
