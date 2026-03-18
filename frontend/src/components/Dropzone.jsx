export default function Dropzone({ dragging, onDragState, onFileSelect, label }) {
  return (
    <label
      className={`flex cursor-pointer flex-col items-center justify-center rounded-[28px] border border-dashed px-6 py-10 text-center transition ${
        dragging
          ? "border-neon-cyan bg-neon-cyan/10"
          : "border-white/15 bg-white/[0.04] hover:border-neon-blue/40 hover:bg-white/[0.06]"
      }`}
      onDragEnter={() => onDragState(true)}
      onDragOver={(event) => {
        event.preventDefault();
        onDragState(true);
      }}
      onDragLeave={() => onDragState(false)}
      onDrop={(event) => {
        event.preventDefault();
        onDragState(false);
        const file = event.dataTransfer.files?.[0];
        if (file) onFileSelect(file);
      }}
    >
      <span className="text-3xl">⬆️</span>
      <p className="mt-3 text-lg font-bold text-white">{label || "Kéo file vào đây"}</p>
      <p className="mt-2 max-w-md text-sm text-slate-400">
        Hỗ trợ `.xlsx`, `.xls`, `.csv`. Dữ liệu sẽ được chuẩn hóa và lưu vào SQLite ngay sau khi tải lên.
      </p>
      <input
        className="hidden"
        type="file"
        accept=".xlsx,.xls,.csv"
        onChange={(event) => {
          const file = event.target.files?.[0];
          if (file) onFileSelect(file);
          event.target.value = "";
        }}
      />
    </label>
  );
}

