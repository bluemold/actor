package bluemold.io

import java.nio.ByteBuffer
import java.nio.charset.Charset

object Serial {
  def getChar(bytes: Array[Byte], offset: Int): Char = {
    var index = offset
    var ret: Int = 0
    val endIndex: Int = index + 2
    while (index < endIndex) {
      ret <<= 8
      ret |= (bytes(index)&0xff)
      index += 1
    }
    ret.asInstanceOf[Char]
  }

  def putChar(bytes: Array[Byte], offset: Int, value: Char) {
    var index = offset
    var shifted: Int = value
    val endIndex: Int = index + 2
    while (index < endIndex) {
      bytes(index) = ((shifted & 0xFF00) >>> 8).asInstanceOf[Byte]
      index += 1
      shifted <<= 8
    }
  }

  def getShort(bytes: Array[Byte], offset: Int): Short = {
    var index = offset
    var ret: Int = 0
    val endIndex: Int = index + 2
    while (index < endIndex) {
      ret <<= 8
      ret |= (bytes(index)&0xff)
      index += 1
    }
    ret.asInstanceOf[Short]
  }

  def putShort(bytes: Array[Byte], offset: Int, value: Short) {
    var index = offset
    var shifted: Int = value
    val endIndex: Int = index + 2
    while (index < endIndex) {
      bytes(index) = ((shifted & 0xFF00) >>> 8).asInstanceOf[Byte]
      index += 1
      shifted <<= 8
    }
  }

  def getInt(bytes: Array[Byte], offset: Int): Int = {
    var index = offset
    var ret: Int = 0
    val endIndex: Int = index + 4
    while (index < endIndex) {
      ret <<= 8
      ret |= (bytes(index)&0xff)
      index += 1
    }
    ret
  }

  def putInt(bytes: Array[Byte], offset: Int, value: Int) {
    var index = offset
    var shifted = value
    val endIndex: Int = index + 4
    while (index < endIndex) {
      bytes(index) = ((shifted & 0xFF000000) >>> 24).asInstanceOf[Byte]
      index += 1
      shifted <<= 8
    }
  }

  def getLong(bytes: Array[Byte], offset: Int): Long = {
    var index = offset
    var ret: Long = 0L
    val endIndex: Int = index + 8
    while (index < endIndex) {
      ret <<= 8
      ret |= (bytes(index)&0xff)
      index += 1
    }
    ret
  }

  def putLong(bytes: Array[Byte], offset: Int, value: Long) {
    var index = offset
    var shifted = value
    val endIndex: Int = index + 8
    while (index < endIndex) {
      bytes(index) = ((shifted & 0xFF00000000000000L) >>> 56).asInstanceOf[Byte]
      index += 1
      shifted <<= 8
    }
  }

  def getBoolean(buffer: ByteBuffer): Boolean = buffer.get() != 0
  def getBoolean(buffer: ByteBuffer, offset: Int): Boolean = buffer.get(offset) != 0   
  def putBoolean(buffer: ByteBuffer, value: Boolean) { buffer.put( ( if( value ) 1 else 0 ): Byte ) }
  def putBoolean(buffer: ByteBuffer, offset: Int, value: Boolean) { buffer.put( offset, ( if( value ) 1 else 0 ): Byte ) }

  def getChar(buffer: ByteBuffer): Char = buffer.getChar
  def getChar(buffer: ByteBuffer, offset: Int): Char = buffer.getChar(offset)
  def putChar(buffer: ByteBuffer, value: Char) { buffer.putChar(value) }
  def putChar(buffer: ByteBuffer, offset: Int, value: Char) { buffer.putChar(offset,value) }

  def getShort(buffer: ByteBuffer): Short = buffer.getShort
  def getShort(buffer: ByteBuffer, offset: Int): Short = buffer.getShort(offset)
  def putShort(buffer: ByteBuffer, value: Short) { buffer.putShort(value) }
  def putShort(buffer: ByteBuffer, offset: Int, value: Short) { buffer.putShort(offset,value) }

  def getInt(buffer: ByteBuffer): Int = buffer.getInt
  def getInt(buffer: ByteBuffer, offset: Int): Int = buffer.getInt(offset)
  def putInt(buffer: ByteBuffer, value: Int) { buffer.putInt(value) }
  def putInt(buffer: ByteBuffer, offset: Int, value: Int) { buffer.putInt(offset,value) }

  def getLong(buffer: ByteBuffer): Long = buffer.getLong
  def getLong(buffer: ByteBuffer, offset: Int): Long = buffer.getLong(offset)
  def putLong(buffer: ByteBuffer, value: Long) { buffer.putLong(value) }
  def putLong(buffer: ByteBuffer, offset: Int, value: Long) { buffer.putLong(offset,value) }

  def size(value: ByteBuffer): Int = 4 + value.remaining()
  def getByteBuffer(buffer: ByteBuffer): ByteBuffer = {
    val size = buffer.getInt
    if ( buffer.hasArray ) {
      val start = buffer.position()
      buffer.position( start + size )
      ByteBuffer.wrap( buffer.array(), start, size )
    }
    else {
      val buf = new Array[Byte](size)
      buffer.get( buf )
      ByteBuffer.wrap( buf )
    }
  }
  def getByteBuffer(buffer: ByteBuffer, offset: Int): ByteBuffer = {
    val size = buffer.getInt( offset )
    if ( buffer.hasArray )
      ByteBuffer.wrap( buffer.array(), offset + 4, size )
    else {
      val buf = new Array[Byte](size)
      var i = 0
      var pos = buffer.position()
      while ( i < size ) {
        buf(i) = buffer.get(pos)
        i+=1
        pos+=1
      }
      ByteBuffer.wrap( buf )
    }
  }
  def putByteBuffer(buffer: ByteBuffer, value: ByteBuffer) {
    val size = value.remaining()
    buffer.putInt(size)
    if ( value.hasArray )
      buffer.put(value.array(),value.position(),size)
    else
      buffer.put(value.duplicate())
  }
  def putByteBuffer(buffer: ByteBuffer, offset: Int, value: ByteBuffer) {
    val size = value.remaining()
    val dup = buffer.duplicate()
    dup.position(offset)
    dup.putInt(size)
    if ( value.hasArray )
      dup.put(value.array(),value.position(),size)
    else
      dup.put(value.duplicate())
  }

  val MIN_HIGH = 55296
  val MAX_HIGH = 56319
  val MIN_LOW = 56320
  val MAX_LOW = 57343
  val MIN_UCS4 = 65536
  val MAX_UCS4 = 1114111
  final def isLow(ch:Int) = (MIN_LOW <= ch) && (ch <= MAX_LOW)
  final def isHigh(ch:Int) = (MIN_HIGH <= ch) && (ch <= MAX_HIGH)
  final def neededFor(uc:Int) = (MIN_UCS4 <= uc) && (uc <= MAX_UCS4)
  final def low(uc:Int) = (0xdc00 | ((uc - MIN_UCS4) & 0x3ff)).toChar
  final def high(uc:Int) = (0xd800 | (((uc - MIN_UCS4) >> 10) & 0x3ff)).toChar

  def encodeLength(target: CharSequence): Int = {
    if (target == null) return 0
    val len: Int = target.length
    if (len == 0) return 0
    var bCount: Int = 0
    var cIndex: Int = 0
    while (cIndex < len) {
      val ch: Int = target.charAt(cIndex)
      if (ch < 0x80) bCount += 1
      else if (ch < 0x800) bCount += 2
      else if (isHigh(ch)) {
        cIndex += 1
        if (cIndex < len) {
          val nch: Int = target.charAt(cIndex)
          if (isLow(nch))
            bCount += 4
          else { bCount += 1; cIndex -= 1 }
        }
        else { bCount += 1; cIndex -= 1 }
      }
      else if (isLow(ch)) {
        bCount += 1; bCount - 1
      }
      else bCount += 3
      cIndex += 1
    }
    bCount
  }

  def encode(target: CharSequence): Array[Byte] = {
    if (target == null) return null
    val len: Int = target.length
    if (len == 0) return new Array[Byte](0)
    val safeLen: Int = len << 2
    val ret: Array[Byte] = new Array[Byte](safeLen)
    var cIndex: Int = 0
    var bIndex: Int = 0
    while (cIndex < len) {
      val ch: Int = target.charAt(cIndex)
      if (ch < 0x80) {
        ret(bIndex) = (ch & 0x7f).asInstanceOf[Byte]
        bIndex += 1
      }
      else if (ch < 0x800) {
        ret(bIndex) = ((ch >>> 6) | 0xc0).asInstanceOf[Byte]
        ret(bIndex+1) = ((ch & 0x3f) | 0x80).asInstanceOf[Byte]
        bIndex += 2
      }
      else if (isHigh(ch)) {
        cIndex += 1
        if (cIndex < len) {
          val nch: Int = target.charAt(cIndex)
          if (isLow(nch)) {
            ret(bIndex) = ((ch >>> 18) | 0xf0).asInstanceOf[Byte]
            ret(bIndex+1) = (((ch >>> 12) & 0x3f) | 0x80).asInstanceOf[Byte]
            ret(bIndex+2) = (((ch >>> 6) & 0x3f) | 0x80).asInstanceOf[Byte]
            ret(bIndex+3) = ((ch & 0x3f) | 0x80).asInstanceOf[Byte]
            bIndex += 4
          }
          else {
            cIndex -= 1
            ret(bIndex) = '?'
            bIndex += 1
          }
        }
        else {
          cIndex -= 1
          ret(bIndex) = '?'
          bIndex += 1
        }
      }
      else if (isLow(ch)) {
        ret(bIndex) = '?'
        bIndex += 1
      }
      else {
        ret(bIndex) = ((ch >>> 12) | 0xe0).asInstanceOf[Byte]
        ret(bIndex+1) = (((ch >>> 6) & 0x3f) | 0x80).asInstanceOf[Byte]
        ret(bIndex+2) = ((ch & 0x3f) | 0x80).asInstanceOf[Byte]
        bIndex += 3
      }
      cIndex += 1
    }
    if (bIndex < safeLen) {
      val newRet: Array[Byte] = new Array[Byte](bIndex)
      System.arraycopy(ret, 0, newRet, 0, bIndex)
      newRet
    }
    else ret
  }

  def encode( ret: Array[Byte], target: CharSequence ) {
    if (target == null) return
    val len: Int = target.length
    if (len == 0) return
    var cIndex: Int = 0
    var bIndex: Int = 0
    while (cIndex < len) {
      val ch: Int = target.charAt(cIndex)
      if (ch < 0x80) {
        ret(bIndex) = (ch & 0x7f).asInstanceOf[Byte]
        bIndex += 1
      } 
      else if (ch < 0x800) {
        ret(bIndex) = ((ch >>> 6) | 0xc0).asInstanceOf[Byte]
        ret(bIndex+1) = ((ch & 0x3f) | 0x80).asInstanceOf[Byte]
        bIndex += 2
      }
      else if (isHigh(ch)) {
        cIndex += 1
        if (cIndex < len) {
          val nch: Int = target.charAt(cIndex)
          if (isLow(nch)) {
            ret(bIndex) = ((ch >>> 18) | 0xf0).asInstanceOf[Byte]
            ret(bIndex+1) = (((ch >>> 12) & 0x3f) | 0x80).asInstanceOf[Byte]
            ret(bIndex+2) = (((ch >>> 6) & 0x3f) | 0x80).asInstanceOf[Byte]
            ret(bIndex+3) = ((ch & 0x3f) | 0x80).asInstanceOf[Byte]
            bIndex += 4
          }
          else {
            cIndex -= 1
            ret({
              bIndex += 1; bIndex - 1
            }) = '?'
          }
        }
        else {
          cIndex -= 1
          ret({
            bIndex += 1; bIndex - 1
          }) = '?'
        }
      }
      else if (isLow(ch)) ret({ bIndex += 1; bIndex - 1 }) = '?'
      else {
        ret(bIndex) = ((ch >>> 12) | 0xe0).asInstanceOf[Byte]
        ret(bIndex+1) = (((ch >>> 6) & 0x3f) | 0x80).asInstanceOf[Byte]
        ret(bIndex+2) = ((ch & 0x3f) | 0x80).asInstanceOf[Byte]
        bIndex += 3
      }
      cIndex += 1
    }
  }

  def encode( buffer: ByteBuffer, target: CharSequence ) {
    if (target == null) return
    val len: Int = target.length
    if (len == 0) return
    var cIndex: Int = 0
    while (cIndex < len) {
      val ch: Int = target.charAt(cIndex)
      if (ch < 0x80) buffer.put((ch & 0x7f).asInstanceOf[Byte])
      else if (ch < 0x800) {
        buffer.put(((ch >>> 6) | 0xc0).asInstanceOf[Byte])
        buffer.put(((ch & 0x3f) | 0x80).asInstanceOf[Byte])
      }
      else if (isHigh(ch)) {
        cIndex += 1
        if (cIndex < len) {
          val nch: Int = target.charAt(cIndex)
          if (isLow(nch)) {
            buffer.put(((ch >>> 18) | 0xf0).asInstanceOf[Byte])
            buffer.put((((ch >>> 12) & 0x3f) | 0x80).asInstanceOf[Byte])
            buffer.put((((ch >>> 6) & 0x3f) | 0x80).asInstanceOf[Byte])
            buffer.put(((ch & 0x3f) | 0x80).asInstanceOf[Byte])
          }
          else {
            cIndex -= 1
            buffer.put('?'.asInstanceOf[Byte])
          }
        }
        else {
          cIndex -= 1
          buffer.put('?'.asInstanceOf[Byte])
        }
      }
      else if (isLow(ch)) buffer.put('?'.asInstanceOf[Byte])
      else {
        buffer.put(((ch >>> 12) | 0xe0).asInstanceOf[Byte])
        buffer.put((((ch >>> 6) & 0x3f) | 0x80).asInstanceOf[Byte])
        buffer.put(((ch & 0x3f) | 0x80).asInstanceOf[Byte])
      }
      cIndex += 1
    }
  }

  private def isMalformed2(b1: Int, b2: Int): Boolean = {
    (b1 & 0x1e) == 0 || (b2 & 0xc0) != 0x80
  }

  private def isMalformed3(b1: Int, b2: Int, b3: Int): Boolean = {
    (b1 == 0xe0.asInstanceOf[Byte] && (b2 & 0xe0) == 0x80) || (b2 & 0xc0) != 0x80 || (b3 & 0xc0) != 0x80
  }

  private def isMalformed4(b2: Int, b3: Int, b4: Int): Boolean = {
    (b2 & 0xc0) != 0x80 || (b3 & 0xc0) != 0x80 || (b4 & 0xc0) != 0x80
  }

  def decodeLength(bytes: Array[Byte]): Int = {
    if (bytes == null) return 0
    decodeLength(bytes, 0, bytes.length)
  }

  def decodeLength(bytes: Array[Byte], offset: Int, len: Int): Int = {
    if (bytes == null) return 0
    if (len == 0) return 0
    val endIndex: Int = offset + len
    var cCount: Int = 0
    var bIndex: Int = 0
    while (bIndex < endIndex) {
      val b1: Int = bytes({ bIndex += 1; bIndex - 1 })
      if (b1 >= 0) cCount += 1
      else if ((b1 & 0xe0) == 0xc0) {
        bIndex += 1
        cCount += 1
      }
      else if ((b1 & 0xf0) == 0xe0) {
        bIndex += 2
        cCount += 1
      }
      else if ((b1 & 0xf8) == 0xf0) {
        val b2: Int = bytes({ bIndex += 1; bIndex - 1 })
        val b3: Int = bytes({ bIndex += 1; bIndex - 1 })
        val b4: Int = bytes({ bIndex += 1; bIndex - 1 })
        if (isMalformed4(b2, b3, b4)) cCount += 1
        else cCount += 2
      }
      else cCount += 1
    }
    cCount
  }

  def decodeLength(buffer: ByteBuffer, len: Int): Int = {
    if (buffer == null) return 0
    if (len == 0) return 0
    val offset: Int = buffer.position
    val endIndex: Int = offset + len
    var cCount: Int = 0
    var bIndex: Int = 0
    while (bIndex < endIndex) {
      val b1: Int = buffer.get({ bIndex += 1; bIndex - 1 })
      if (b1 >= 0) cCount += 1
      else if ((b1 & 0xe0) == 0xc0) {
        bIndex += 1
        cCount += 1
      }
      else if ((b1 & 0xf0) == 0xe0) {
        bIndex += 2
        cCount += 1
      }
      else if ((b1 & 0xf8) == 0xf0) {
        val b2: Int = buffer.get({ bIndex += 1; bIndex - 1 })
        val b3: Int = buffer.get({ bIndex += 1; bIndex - 1 })
        val b4: Int = buffer.get({ bIndex += 1; bIndex - 1 })
        if (isMalformed4(b2, b3, b4)) cCount += 1
        else cCount += 2
      }
      else cCount += 1
    }
    cCount
  }

  def decode(bytes: Array[Byte]): String = {
    if (bytes == null) return null
    new String(bytes, UTF8)
  }

  def decode(bytes: Array[Byte], offset: Int, len: Int): String = {
    if (bytes == null) return null
    if (len == 0) return ""
    new String(bytes, offset, len, UTF8)
  }

  def decode(buffer: ByteBuffer, len: Int): String = {
    if (buffer == null) return null
    if (len == 0) return ""
    val chars: Array[Char] = new Array[Char](len)
    val endPosition: Int = buffer.position + len
    var cIndex: Int = 0
    while (buffer.position < endPosition) {
      val b1: Int = buffer.get
      if (b1 >= 0) {
        chars(cIndex) = b1.asInstanceOf[Char]
        cIndex += 1
      }
      else if ((b1 & 0xe0) == 0xc0) {
        val b2: Int = buffer.get
        chars(cIndex) = if (isMalformed2(b1, b2)) '?'
          else (0x0f80 ^ (b1 << 6) ^ b2).asInstanceOf[Char]
        cIndex += 1
      }
      else if ((b1 & 0xf0) == 0xe0) {
        val b2: Int = buffer.get
        val b3: Int = buffer.get
        chars(cIndex) = if (isMalformed3(b1, b2, b3)) '?'
          else (0x1f80 ^ (b1 << 12) ^ (b2 << 6) ^ b3).asInstanceOf[Char]
        cIndex += 1
      }
      else if ((b1 & 0xf8) == 0xf0) {
        val b2: Int = buffer.get
        val b3: Int = buffer.get
        val b4: Int = buffer.get
        val uc: Int = ((b1 & 0x07) << 18) | ((b2 & 0x3f) << 12) | ((b3 & 0x3f) << 6) | (b4 & 0x3f)
        if (isMalformed4(b2, b3, b4) || !neededFor(uc)) {
          chars(cIndex) = '?'
          cIndex += 1
        }
        else {
          chars(cIndex) = high(uc)
          chars(cIndex+1) = low(uc)
          cIndex += 2
        }
      }
      else {
        chars(cIndex) = '?'
        cIndex += 1
      }
    }
    new String(chars, 0, cIndex)
  }

  def decode(buffer: ByteBuffer, offset: Int, len: Int): String = {
    if (buffer == null) return null
    if (len == 0) return ""
    val chars: Array[Char] = new Array[Char](len)
    val endIndex: Int = offset + len
    var cIndex = 0
    var bIndex = offset
    while ( bIndex < endIndex) {
      val b1: Int = buffer.get(bIndex)
      bIndex+=1
      if (b1 >= 0) {
        chars(cIndex) = b1.asInstanceOf[Char]
        cIndex += 1
      }
      else if ((b1 & 0xe0) == 0xc0) {
        val b2: Int = buffer.get(bIndex)
        bIndex+=1
        chars(cIndex) = if (isMalformed2(b1, b2)) '?'
          else (0x0f80 ^ (b1 << 6) ^ b2).asInstanceOf[Char]
        cIndex+=1
      }
      else if ((b1 & 0xf0) == 0xe0) {
        val b2: Int = buffer.get(bIndex)
        val b3: Int = buffer.get(bIndex+1)
        bIndex+=2
        chars(cIndex) = if (isMalformed3(b1, b2, b3)) '?'
          else (0x1f80 ^ (b1 << 12) ^ (b2 << 6) ^ b3).asInstanceOf[Char]
        cIndex+=1
      }
      else if ((b1 & 0xf8) == 0xf0) {
        val b2: Int = buffer.get(bIndex)
        val b3: Int = buffer.get(bIndex+1)
        val b4: Int = buffer.get(bIndex+2)
        bIndex+=3
        val uc: Int = ((b1 & 0x07) << 18) | ((b2 & 0x3f) << 12) | ((b3 & 0x3f) << 6) | (b4 & 0x3f)
        if (isMalformed4(b2, b3, b4) || !neededFor(uc)) chars(cIndex) = '?'
        else {
          chars(cIndex) = high(uc)
          cIndex+=1
          chars(cIndex) = low(uc)
        }
        cIndex+=1
      }
      else {
        chars(cIndex) = '?'
        cIndex+=1
      }
    }
    new String(chars, 0, cIndex)
  }

  final val UTF8: Charset = Charset.forName("UTF-8")
  
  def apply(buffer: ByteBuffer) = new Serial(buffer: ByteBuffer)
}

class Serial( buffer: ByteBuffer ) {
  def getBoolean(): Boolean = buffer.get() != 0
  def putBoolean(value: Boolean) = { buffer.put( ( if( value ) 1 else 0 ): Byte ); this }

  def getChar(): Char = buffer.getChar
  def putChar(value: Char) = { buffer.putChar(value); this }

  def getShort(): Short = buffer.getShort
  def putShort(value: Short) = { buffer.putShort(value); this }

  def getInt(): Int = buffer.getInt
  def putInt(value: Int) = { buffer.putInt(value); this }

  def getLong(): Long = buffer.getLong
  def putLong(value: Long) = { buffer.putLong(value); this }
  
  def getByteBuffer(): ByteBuffer = Serial.getByteBuffer(buffer)
  def putByteBuffer(value:ByteBuffer) = { Serial.putByteBuffer(buffer,value); this }
  
  def conditional( success: Boolean ) = if ( success ) this else new SkipNextSerial( this )
}

class SkipNextSerial( serial: Serial ) extends Serial(null) {
  override def putBoolean(value: Boolean) = serial
  override def putChar(value: Char) = serial
  override def putShort(value: Short) = serial
  override def putInt(value: Int) = serial
  override def putLong(value: Long) = serial
  override def putByteBuffer(value: ByteBuffer) = serial
  override def conditional(success: Boolean) = this
}