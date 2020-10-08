/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.utils

import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

object CryptHelpers {

  private val ALGORITHM_NAME = "AES/GCM/NoPadding"
  private val ALGORITHM_NONCE_SIZE = 12
  private val ALGORITHM_TAG_SIZE = 256

  /**
   * Decrypts base64 encoded AES encrypted string.
   *
   * @param base64NonceAndCiphertext Base64 encoded AES encrypted string containing nonce and ciphertext
   * @param key key encrytion key value
   *
   * @return decrypted string
   *
   * @throws IllegalArgumentException
   * @throws InvalidAlgorithmParameterException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws NoSuchPaddingException
   *
   */
  @throws(classOf[IllegalArgumentException])
  @throws(classOf[InvalidAlgorithmParameterException])
  @throws(classOf[InvalidKeyException])
  @throws(classOf[NoSuchAlgorithmException])
  @throws(classOf[NoSuchPaddingException])
  def decryptString(base64NonceAndCiphertext: String, key: String): String = {
    // decode and descrypt..
    val nonceAndCiphertext = Base64.getDecoder().decode(base64NonceAndCiphertext);
    return new String(decrypt(nonceAndCiphertext, key.getBytes()), StandardCharsets.UTF_8);
  }

  /**
   * Decrypts AES encrypted byte array.
   *
   * @param nonceAndCiphertext AES encrypted byte array containing nonce and ciphertext
   * @param key key encrytion key value
   *
   * @return decrypted byte array
   *
   * @throws IllegalArgumentException
   * @throws InvalidAlgorithmParameterException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws NoSuchPaddingException
   *
   *
   */
  @throws(classOf[IllegalArgumentException])
  @throws(classOf[InvalidAlgorithmParameterException])
  @throws(classOf[InvalidKeyException])
  @throws(classOf[NoSuchAlgorithmException])
  @throws(classOf[NoSuchPaddingException])
  private def decrypt(nonceAndCiphertext: Array[Byte], key: Array[Byte]): Array[Byte] = {
    // retrieve nonce and ciphertext
    val nonce = new Array[Byte](ALGORITHM_NONCE_SIZE)
    val ciphertext = new Array[Byte](nonceAndCiphertext.length - ALGORITHM_NONCE_SIZE);
    Array.copy(nonceAndCiphertext, 0, nonce, 0, nonce.size)
    Array.copy(nonceAndCiphertext, nonce.size, ciphertext, 0, ciphertext.size)

    // create cipher instance and initialize
    val cipher = Cipher.getInstance(ALGORITHM_NAME);
    cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"), new GCMParameterSpec(ALGORITHM_TAG_SIZE, nonce));

    // decrypt..
    return cipher.doFinal(ciphertext);
  }
}
