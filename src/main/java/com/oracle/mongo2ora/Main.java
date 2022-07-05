package com.oracle.mongo2ora;

import com.oracle.mongo2ora.util.Kernel32;
import net.rubygrapefruit.platform.Native;
import net.rubygrapefruit.platform.internal.Platform;
import net.rubygrapefruit.platform.terminal.TerminalInput;
import net.rubygrapefruit.platform.terminal.TerminalOutput;
import net.rubygrapefruit.platform.terminal.TerminalSize;
import net.rubygrapefruit.platform.terminal.Terminals;

import java.io.PrintStream;

public class Main {
	public static boolean ENABLE_COLORS = true;

	public static void main(String[] args) {
		// will run only on Windows OS
		Kernel32.init();

		System.out.println("Colors enabled: "+ENABLE_COLORS);

//		System.out.println(Platform.current().isWindows());
		Terminals terminals = Native.get(Terminals.class).withAnsiOutput();
		if(!terminals.isTerminal(Terminals.Output.Stdout)) {
			return;
		}

/*		final TerminalOutput terminalOutput = terminals.withAnsiOutput().getTerminal(Terminals.Output.Stdout);
		terminalOutput.bold().write("Hello").cursorDown(2).write("World!").newline();
*/

		System.out.println();
		boolean stdoutIsTerminal = terminals.isTerminal(Terminals.Output.Stdout);
		boolean stderrIsTerminal = terminals.isTerminal(Terminals.Output.Stderr);
		boolean stdinIsTerminal = terminals.isTerminalInput();
		System.out.println("* Stdout: " + (stdoutIsTerminal ? "terminal" : "not a terminal"));
		System.out.println("* Stderr: " + (stderrIsTerminal ? "terminal" : "not a terminal"));
		System.out.println("* Stdin: " + (stdinIsTerminal ? "terminal" : "not a terminal"));
		if (stdoutIsTerminal) {
			TerminalOutput terminal = terminals.getTerminal(Terminals.Output.Stdout);
			System.setOut(new PrintStream(terminal.getOutputStream(), true));
			TerminalSize terminalSize = terminal.getTerminalSize();
			System.out.println("* Terminal implementation: " + terminal);
			System.out.println("* Terminal size: " + terminalSize.getCols() + " cols x " + terminalSize.getRows() + " rows");
			System.out.println("* Text attributes: " + (terminal.supportsTextAttributes() ? "yes" : "no"));
			System.out.println("* Color: " + (terminal.supportsColor() ? "yes" : "no"));
			System.out.println("* Cursor motion: " + (terminal.supportsCursorMotion() ? "yes" : "no"));
			System.out.println("* Cursor visibility: " + (terminal.supportsCursorVisibility() ? "yes" : "no"));
			if (stdinIsTerminal) {
				TerminalInput terminalInput = terminals.getTerminalInput();
				System.out.println("* Terminal input: " + terminalInput);
				System.out.println("* Raw mode: " + (terminalInput.supportsRawMode() ? "yes" : "no"));
			}
			System.out.println();
			System.out.println("TEXT ATTRIBUTES");
			System.out.print("[normal]");
			terminal.bold();
			System.out.print(" [bold]");
			terminal.dim();
			System.out.print(" [bold+dim]");
			terminal.normal();
			System.out.print(" [normal]");
			terminal.dim();
			System.out.println(" [dim]");
			terminal.normal();
			System.out.println();

			System.out.println("COLORS");
			System.out.println("bold      bold+dim  bright    normal    dim");
			for (TerminalOutput.Color color : TerminalOutput.Color.values()) {
				terminal.foreground(color);
				terminal.bold();
				System.out.print(String.format("%-9s ", "[" + color.toString().toLowerCase() + "]"));
				terminal.dim();
				System.out.print(String.format("%-9s ", "[" + color.toString().toLowerCase() + "]"));
				terminal.normal();
				terminal.bright();
				System.out.print(String.format("%-9s ", "[" + color.toString().toLowerCase() + "]"));
				terminal.normal();
				System.out.print(String.format("%-9s ", "[" + color.toString().toLowerCase() + "]"));
				terminal.dim();
				System.out.print(String.format("%-9s ", "[" + color.toString().toLowerCase() + "]"));
				terminal.normal();
				System.out.println();
			}
			System.out.println();

			terminal.reset();

			if (terminal.supportsCursorMotion()) {
				System.out.println("CURSOR MOVEMENT");
				System.out.println("                    ");
				System.out.println("                    ");
				System.out.print("[delete me]");

				terminal.cursorLeft(11);
				terminal.cursorUp(1);
				terminal.cursorRight(10);
				System.out.print("[4]");
				terminal.cursorUp(1);
				terminal.cursorLeft(3);
				System.out.print("[2]");
				terminal.cursorLeft(13);
				System.out.print("[1]");
				terminal.cursorLeft(3);
				terminal.cursorUp(2);
				terminal.cursorDown(3);
				System.out.print("[3]");
				terminal.cursorDown(1);
				terminal.cursorStartOfLine();
				terminal.foreground(TerminalOutput.Color.Blue).bold();
				System.out.print("done");
				terminal.clearToEndOfLine();
				System.out.println("!");
				terminal.reset();
				System.out.println();
			}

			System.out.print("Can write unicode: ");
			terminal.bold().foreground(TerminalOutput.Color.Blue).write("\u03B1\u03B2\u03B3");
			terminal.normal();
			System.out.print(' ');
			terminal.foreground(TerminalOutput.Color.Green);
			System.out.println("\u2714");
			terminal.reset();
			System.out.println();
		}
	}
}
