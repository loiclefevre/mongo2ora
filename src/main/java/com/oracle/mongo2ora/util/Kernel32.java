package com.oracle.mongo2ora.util;

import com.oracle.mongo2ora.Main;
import org.graalvm.nativeimage.ImageInfo;
import org.graalvm.nativeimage.Platform;
import org.graalvm.nativeimage.Platforms;
import org.graalvm.nativeimage.UnmanagedMemory;
import org.graalvm.nativeimage.c.CContext;
import org.graalvm.nativeimage.c.function.CFunction;
import org.graalvm.nativeimage.c.struct.CField;
import org.graalvm.nativeimage.c.struct.CFieldAddress;
import org.graalvm.nativeimage.c.struct.CPointerTo;
import org.graalvm.nativeimage.c.struct.CStruct;
import org.graalvm.nativeimage.c.struct.SizeOf;
import org.graalvm.word.Pointer;
import org.graalvm.word.PointerBase;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@CContext(Kernel32.Directives.class)
public class Kernel32 {

	public static void init() {
		if (ImageInfo.inImageRuntimeCode() && Platform.includedIn(Platform.WINDOWS.class)) {
			final BigDecimal OSVersion = new BigDecimal(System.getProperty("os.version"));
			final String VMName = System.getProperty("java.vm.name");
			if (OSVersion.doubleValue() >= 10.0d || (OSVersion.doubleValue() >= 6.2d && "Substrate VM".equalsIgnoreCase(VMName))) {
				try {
					final Pointer handle = Kernel32.getStdHandle(-11);
					Kernel32.setConsoleMode(handle, 7);
					Main.ENABLE_COLORS = true;

					// Now retrieving current console size
/*					ConsoleScreenBufferInfo_POINTER infoPtr = UnmanagedMemory.calloc(SizeOf.get(CONSOLE_SCREEN_BUFFER_INFO.class));

					int ret = Kernel32.getConsoleScreenBufferInfo(handle, infoPtr);
					System.out.println("Returned: "+ret);
					System.out.println("Sizeof=" + SizeOf.get(CONSOLE_SCREEN_BUFFER_INFO.class));
					System.out.println("infoPtr=" + infoPtr.rawValue());

					CONSOLE_SCREEN_BUFFER_INFO info = infoPtr.read();

					System.out.println("2");

					System.out.println(info.wAttributes());
					System.out.println(info.dwSize().getX());

					UnmanagedMemory.free(infoPtr);
*/				}
				catch (Throwable t) {
					Main.ENABLE_COLORS = false;
					t.printStackTrace();
				}
			}
		}
	}

	@Platforms(Platform.WINDOWS.class)
	static final class Directives implements CContext.Directives {
		@Override
		public List<String> getHeaderFiles() {
			if (Platform.includedIn(Platform.WINDOWS.class)) {
				return Arrays.asList("<windows.h>");
			}
			else {
				throw new IllegalStateException("Unsupported OS");
			}
		}

		@Override
		public boolean isInConfiguration() {
			return Platform.includedIn(Platform.WINDOWS.class);
		}

		@Override
		public List<String> getMacroDefinitions() {
			return Arrays.asList("_WIN64");
		}

		@Override
		public List<String> getLibraries() {
			return Arrays.asList("Kernel32");
		}
	}

	/*
	HANDLE WINAPI GetStdHandle(
	  _In_ DWORD nStdHandle
	);
	*/
	@CFunction("GetStdHandle")
	@Platforms(Platform.WINDOWS.class)
	static native Pointer getStdHandle(int nStdHandle);

	/*
	BOOL WINAPI SetConsoleMode(
	  _In_ HANDLE hConsoleHandle,
	  _In_ DWORD  dwMode
	);
	 */
	@CFunction("SetConsoleMode")
	@Platforms(Platform.WINDOWS.class)
	static native int setConsoleMode(Pointer hConsoleHandle, int dwMode);

	/*
	BOOL WINAPI GetConsoleScreenBufferInfo(
      _In_  HANDLE                      hConsoleOutput,
      _Out_ PCONSOLE_SCREEN_BUFFER_INFO lpConsoleScreenBufferInfo
    );
	 */
	@CFunction("GetConsoleScreenBufferInfo")
	@Platforms(Platform.WINDOWS.class)
	static native int getConsoleScreenBufferInfo(Pointer hConsoleHandle, ConsoleScreenBufferInfo_POINTER info);

	/*

     * http://msdn.microsoft.com/en-us/library/ms682093%28VS.85%29.aspx
     *
	@JniClass(flags={ClassFlag.STRUCT,TYPEDEF}, conditional="defined(_WIN32) || defined(_WIN64)")
	public static class CONSOLE_SCREEN_BUFFER_INFO {

		static {
			LIBRARY.load();
			init();
		}

		@JniMethod(flags={CONSTANT_INITIALIZER})
		private static final native void init();
		@JniField(flags={CONSTANT}, accessor="sizeof(CONSOLE_SCREEN_BUFFER_INFO)")
		public static int SIZEOF;

		@JniField(accessor="dwSize")
		public COORD      size = new COORD();
		@JniField(accessor="dwCursorPosition")
		public COORD      cursorPosition = new COORD();
		@JniField(accessor="wAttributes")
		public short      attributes;
		@JniField(accessor="srWindow")
		public SMALL_RECT window = new SMALL_RECT();
		@JniField(accessor="dwMaximumWindowSize")
		public COORD      maximumWindowSize = new COORD();

		public int windowWidth() {
			return window.width() + 1;
		}

		public int windowHeight() {
			return window.height() + 1;
		}
	}
	 */

	/*
	typedef struct _CONSOLE_SCREEN_BUFFER_INFO {
  COORD      dwSize;
  COORD      dwCursorPosition;
  WORD       wAttributes;
  SMALL_RECT srWindow;
  COORD      dwMaximumWindowSize;
} CONSOLE_SCREEN_BUFFER_INFO;
	 */
	@CContext(Kernel32.Directives.class)
	@CStruct
	public interface CONSOLE_SCREEN_BUFFER_INFO extends PointerBase {
		@CFieldAddress("dwSize")
		COORD dwSize();

		@CFieldAddress("dwCursorPosition")
		COORD dwCursorPosition();

		@CField("wAttributes")
		short wAttributes();

		@CFieldAddress("srWindow")
		SMALL_RECT srWindow();

		@CFieldAddress("dwMaximumWindowSize")
		COORD dwMaximumWindowSize();
	}

	@CContext(Kernel32.Directives.class)
	@CPointerTo(CONSOLE_SCREEN_BUFFER_INFO.class)
	public interface ConsoleScreenBufferInfo_POINTER extends PointerBase {
		CONSOLE_SCREEN_BUFFER_INFO read();
	}

	/*
	typedef struct _COORD {
		SHORT X;
		SHORT Y;
	} COORD, *PCOORD;
	 */

	@CContext(Kernel32.Directives.class)
	@CStruct
	public interface COORD extends PointerBase {
		@CField("X")
		short getX();
		@CField("Y")
		short getY();
	}

	/*
typedef struct _SMALL_RECT {
  SHORT Left;
  SHORT Top;
  SHORT Right;
  SHORT Bottom;
} SMALL_RECT;
	 */

	@CContext(Kernel32.Directives.class)
	@CStruct
	public interface SMALL_RECT extends PointerBase {
		@CField("Left")
		short getLeft();
		@CField("Left")
		void setLeft(short Left);
		@CField("Top")
		short getTop();
		@CField("Top")
		void setTop(short Top);
		@CField("Right")
		short getRight();
		@CField("Right")
		void setRight(short Right);
		@CField("Bottom")
		short getBottom();
		@CField("Bottom")
		void setBottom(short Bottom);
	}
}
