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
#include <stdexcept>
#include <vector>

#include "common/exception.h"

namespace bustub {

/**
 * The Matrix type defines a common
 * interface for matrix operations.
 */
template <typename T>
class Matrix {
 protected:
  /**
   * (P0): Add implementation
   *
   * Construct a new Matrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   *
   */
  Matrix(int rows, int cols) {
    rows_ = rows;
    cols_ = cols;
    linear_ = new T[rows * cols];
  }

  /** The number of rows in the matrix */
  int rows_;
  /** The number of columns in the matrix */
  int cols_;

  /**
   * (P0): Allocate the array in the constructor.
   * (P0): Deallocate the array in the destructor.
   * A flattened array containing the elements of the matrix.
   */
  T *linear_;

 public:
  /** @return The number of rows in the matrix */
  virtual int GetRowCount() const = 0;

  /** @return The number of columns in the matrix */
  virtual int GetColumnCount() const = 0;

  /**
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual T GetElement(int i, int j) const = 0;

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual void SetElement(int i, int j, T val) = 0;

  /**
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  virtual void FillFrom(const std::vector<T> &source) = 0;

  /**
   * Destroy a matrix instance.
   * (P0): Add implementation
   */
  virtual ~Matrix() {  // 具有动态分配内存的类析构函数必须是虚函数/纯虚函数
    delete[] linear_;
  }
};

/**
 * The RowMatrix type is a concrete matrix implementation.
 * It implements the interface defined by the Matrix type.
 */
template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  /**
   * (P0): Add implementation
   *
   * Construct a new RowMatrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   */
  RowMatrix(int rows, int cols) : Matrix<T>(rows, cols) {
    data_ = new T *[rows];
    T *p = this->linear_;
    for (int i = 0; i < rows; i++) {
      data_[i] = p;
      p += cols;
    }
  }

  /**
   * (P0): Add implementation
   * @return The number of rows in the matrix
   */
  int GetRowCount() const override { return this->rows_; }

  /**
   * (P0): Add implementation
   * @return The number of columns in the matrix
   */
  int GetColumnCount() const override { return this->cols_; }

  /**
   * (P0): Add implementation
   *
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  T GetElement(int i, int j) const override {
    if (i < 0 || j < 0 || i >= this->GetRowCount() || j >= this->GetColumnCount()) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "oor");
    }
    return data_[i][j];
  }

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  void SetElement(int i, int j, T val) override {
    if (i < 0 || j < 0 || i >= this->GetRowCount() || j >= this->GetColumnCount()) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "oor");
    }
    data_[i][j] = val;
  }

  /**
   * (P0): Add implementation
   *
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  void FillFrom(const std::vector<T> &source) override {
    int m_size = this->cols_ * this->rows_;
    if (static_cast<int>(source.size()) != m_size) {
      ExceptionType t = ExceptionType::OUT_OF_RANGE;
      throw Exception(t, "out of range");
    }
    for (int i = 0; i < m_size; i++) {
      this->linear_[i] = source[i];
    }
  }

  /**
   * (P0): Add implementation
   *
   * Destroy a RowMatrix instance.
   */
  virtual ~RowMatrix() { delete[] data_; }

 private:
  /**
   * A 2D array containing the elements of the matrix in row-major format.
   *
   * (P0):
   * - Allocate the array of row pointers in the constructor.
   * - Use these pointers to point to corresponding elements of the `linear` array.
   * - Don't forget to deallocate the array in the destructor.
   */
  T **data_;
};

/**
 * The RowMatrixOperations class defines operations
 * that may be performed on instances of `RowMatrix`.
 */
template <typename T>
class RowMatrixOperations {
 public:
  /**
   * Compute (`matrixA` + `matrixB`) and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix addition
   */
  static std::unique_ptr<RowMatrix<T>> Add(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    if (matrixA == nullptr || matrixB == nullptr) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    if (matrixA->GetColumnCount() != matrixB->GetColumnCount() || matrixA->GetRowCount() != matrixB->GetRowCount()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    int col = matrixA->GetColumnCount();
    int row = matrixA->GetRowCount();
    std::unique_ptr<RowMatrix<T>> res_ptr(new RowMatrix<T>(row, col));
    for (int i = 0; i < row; i++) {
      for (int j = 0; j < col; j++) {
        res_ptr.get()->SetElement(i, j, matrixA->GetElement(i, j) + matrixB->GetElement(i, j));
      }
    }
    return res_ptr;
  }

  /**
   * Compute the matrix multiplication (`matrixA` * `matrixB` and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix multiplication
   */
  static std::unique_ptr<RowMatrix<T>> Multiply(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    if (matrixA == nullptr || matrixB == nullptr) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    if (matrixA->GetColumnCount() != matrixB->GetRowCount()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    int col = matrixB->GetColumnCount();
    int row = matrixA->GetRowCount();
    int time = matrixA->GetColumnCount();
    if (time == 0) {
      return std::unique_ptr<RowMatrix<T>>(new RowMatrix<T>(0, 0));
    }
    std::unique_ptr<RowMatrix<T>> res_ptr(new RowMatrix<T>(row, col));
    for (int i = 0; i < row; i++) {
      for (int j = 0; j < col; j++) {
        T t = matrixA->GetElement(i, 0) * matrixB->GetElement(0, j);
        for (int k = 1; k < time; k++) {
          t += matrixA->GetElement(i, k) * matrixB->GetElement(k, j);
        }
        res_ptr.get()->SetElement(i, j, t);
      }
    }
    return res_ptr;
  }

  /**
   * Simplified General Matrix Multiply operation. Compute (`matrixA` * `matrixB` + `matrixC`).
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @param matrixC Input matrix
   * @return The result of general matrix multiply
   */
  static std::unique_ptr<RowMatrix<T>> GEMM(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB,
                                            const RowMatrix<T> *matrixC) {
    return Add(Multiply(matrixA, matrixB), matrixC);
  }
};
}  // namespace bustub
