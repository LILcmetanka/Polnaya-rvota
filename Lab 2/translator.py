import re

class JSToPythonTranslator:

    def __init__(self):
        self.indent_level = 0
        self.result = []


    def indent(self) -> str:
        return "    " * self.indent_level

    def add_line(self, line: str):
        if line.strip():
            self.result.append(line)


    def translate_line(self, line: str) -> str:
        line = line.strip()

        if not line:
            return ""

        if line.startswith("//"):
            return self.indent() + "#" + line[2:]

        match = re.match(r'^(var|let|const)\s+(\w+)\s*=\s*(.*);$', line)
        if match:
            return self.indent() + f"{match.group(2)} = {match.group(3)}"

        match = re.match(r'^function\s+(\w+)\s*\((.*?)\)\s*{', line)
        if match:
            name = match.group(1)
            args = match.group(2)
            header = self.indent() + f"def {name}({args}):"
            self.indent_level += 1
            return header

        if line == "}":
            self.indent_level -= 1
            return ""

        match = re.match(r'^if\s*\((.*?)\)\s*{', line)
        if match:
            cond = match.group(1)
            header = self.indent() + f"if {cond}:"
            self.indent_level += 1
            return header

        match = re.match(r'^else\s+if\s*\((.*?)\)\s*{', line)
        if match:
            cond = match.group(1)
            self.indent_level += 1
            return self.indent()[:-4] + f"elif {cond}:"

        match = re.match(r'^else\s*{', line)
        if match:
            self.indent_level += 1
            return self.indent()[:-4] + f"else:"

        match = re.match(r'^while\s*\((.*?)\)\s*{', line)
        if match:
            cond = match.group(1)
            header = self.indent() + f"while {cond}:"
            self.indent_level += 1
            return header

        match = re.match(r'^for\s*\((.*?);(.*?);(.*?)\)\s*{', line)
        if match:
            init = match.group(1).strip()
            cond = match.group(2).strip()
            step = match.group(3).strip()

            init = re.sub(r'^(var|let|const)\s+', '', init)

            header = self.indent() + f"# for ({init}; {cond}; {step})"
            self.indent_level += 1
            return header + "\n" + self.indent() + "while " + cond + ":"

        match = re.match(r'^return\s+(.*);$', line)
        if match:
            return self.indent() + f"return {match.group(1)}"

        match = re.match(r'^console\.log\((.*)\);$', line)
        if match:
            return self.indent() + f"print({match.group(1)})"

        match = re.match(r'^(\w+)\+\+;$', line)
        if match:
            return self.indent() + f"{match.group(1)} += 1" 

        match = re.match(r'^(\w+)\-\-;$', line)
        if match:
            return self.indent() + f"{match.group(1)} -= 1"

        line = line.replace(";", "")
        return self.indent() + line


    def translate_code(self, code: str) -> str:
        self.result = []
        for raw_line in code.splitlines():
            translated = self.translate_line(raw_line)
            if translated.strip():
                self.add_line(translated)
        return "\n".join(self.result)

if __name__ == "__main__":
    with open("program.js", "r", encoding="utf-8") as f:
        js_code = f.read()

    translator = JSToPythonTranslator()
    python_code = translator.translate_code(js_code)

    with open("program.py", "w", encoding="utf-8") as f:
        f.write(python_code)

    print("Результат сохранён в program.py")
