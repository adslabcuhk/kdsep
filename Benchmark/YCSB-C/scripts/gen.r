

args <- commandArgs(trailingOnly = T);

if (length(args) >= 3) {
  theta <- as.numeric(args[1]);
  n <- as.numeric(args[2]);
  m <- as.numeric(args[3]);
} else {
  theta <- 0.9;
  n <- 200 * 1000 * 1000;
  m <- 2 * 1000 * 1000;
}

probs <- 1/(1:n)^theta;
v <- sample(1:n, m, T, probs);
write.table(v, file = "out.data", quote = F, row.names = F, col.names = F);
q()

for (theta in c(0.9, 1, 1.1, 1.2)) {
  probs <- 1/(1:n)^theta;
  v <- sample(1:n, m, T, probs);
  write.table(v, file = paste0("skew", theta, ".data"), quote = F, row.names = F, col.names = F);
}
