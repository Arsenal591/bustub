//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

namespace bustub {

/*
 * The base class defining a Matrix
 */
template <typename T>
class Matrix {
 protected:
  Matrix(int r, int c) : rows(r), cols(c), linear(new T[r * c]) {}

  // # of rows in the matrix
  int rows;
  // # of Columns in the matrix
  int cols;
  // Flattened array containing the elements of the matrix
  // TODO(P0) : Allocate the array in the constructor. Don't forget to free up
  // the array in the destructor.
  T *linear;

 public:
  // Return the # of rows in the matrix
  virtual int GetRows() = 0;

  // Return the # of columns in the matrix
  virtual int GetColumns() = 0;

  // Return the (i,j)th  matrix element
  virtual T GetElem(int i, int j) = 0;

  // Sets the (i,j)th  matrix element to val
  virtual void SetElem(int i, int j, T val) = 0;

  // Sets the matrix elements based on the array arr
  virtual void MatImport(T *arr) = 0;

  // TODO(P0): Add implementation
  virtual ~Matrix() { delete[] linear; }
};

template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  RowMatrix(int r, int c) : Matrix<T>(r, c), data_(new T *[r]) {
    for (int i = 0; i < r; i++) {
      data_[i] = &(this->linear[i * c]);
    }
  }

  int GetRows() override { return this->rows; }

  int GetColumns() override { return this->cols; }

  T GetElem(int i, int j) override { return data_[i][j]; }

  void SetElem(int i, int j, T val) override { data_[i][j] = val; }

  void MatImport(T *arr) override { memcpy(this->linear, arr, sizeof(T) * this->rows * this->cols); }

  ~RowMatrix() override { delete[] data_; }

 private:
  // 2D array containing the elements of the matrix in row-major format
  T **data_;
};

template <typename T>
class RowMatrixOperations {
 public:
  // Compute (mat1 + mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> AddMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                   std::unique_ptr<RowMatrix<T>> mat2) {
    if (!mat1 || !mat2) {
      return nullptr;
    }
    if (mat1->GetRows() != mat2->GetRows()) {
      return nullptr;
    }
    if (mat2->GetColumns() != mat2->GetColumns()) {
      return nullptr;
    }
    auto ptr = std::unique_ptr<RowMatrix<T>>(new RowMatrix<T>(mat1->GetRows(), mat2->GetRows()));
    for (int i = 0; i < mat1->GetRows(); i++) {
      for (int j = 0; j < mat1->GetColumns(); j++) {
        ptr->SetElem(i, j, mat1->GetElem(i, j) + mat2->GetElem(i, j));
      }
    }
    return ptr;
  }

  // Compute matrix multiplication (mat1 * mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> MultiplyMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                        std::unique_ptr<RowMatrix<T>> mat2) {
    if (!mat1 || !mat2) {
      return nullptr;
    }
    int r1 = mat1->GetRows();
    int c1 = mat1->GetColumns();
    int r2 = mat2->GetRows();
    int c2 = mat2->GetColumns();
    if (c1 != r2) {
      return nullptr;
    }

    auto ptr = std::unique_ptr<RowMatrix<T>>(new RowMatrix<T>(r1, c2));
    for (int i = 0; i < r1; i++) {
      for (int j = 0; j < c2; j++) {
        T val{};
        for (int k = 0; k < c1; k++) {
          val += mat1->GetElem(i, k) * mat2->GetElem(k, j);
        }
        ptr->SetElem(i, j, val);
      }
    }
    return ptr;

    return std::unique_ptr<RowMatrix<T>>(nullptr);
  }

  // Simplified GEMM (general matrix multiply) operation
  // Compute (matA * matB + matC). Return nullptr if dimensions mismatch for input matrices
  static std::unique_ptr<RowMatrix<T>> GemmMatrices(std::unique_ptr<RowMatrix<T>> matA,
                                                    std::unique_ptr<RowMatrix<T>> matB,
                                                    std::unique_ptr<RowMatrix<T>> matC) {
    auto multiplied_matrix = std::move(MultiplyMatrices(std::move(matA), std::move(matB)));
    if (multiplied_matrix) {
      return AddMatrices(std::move(multiplied_matrix), std::move(matC));
    }
    return nullptr;
  }
};
}  // namespace bustub
